"""
realestate.com.au Partner API service.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import uuid
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import REA_AGENCY_ID, REA_CLIENT_ID, REA_CLIENT_SECRET

logger = logging.getLogger(__name__)
LISTING_COPY_DISCLAIMER = (
    "Disclaimer: Information is indicative only, subject to change without notice, "
    "and should not be relied on as legal or financial advice. Buyers must make their own enquiries."
)

REA_TOKEN_URL = "https://api.realestate.com.au/oauth/token"
REA_API_BASE = "https://api.realestate.com.au"

_access_token: Optional[str] = None
_token_expiry: Optional[datetime.datetime] = None


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


async def _log_rea_api_call(
    session: Optional[AsyncSession],
    *,
    lead_id: str = "",
    upload_id: str = "",
    listing_id: str = "",
    action: str,
    request_method: str,
    request_path: str,
    request_payload: Any = None,
    response_status_code: Optional[int] = None,
    response_body: Any = None,
    ok: bool = False,
    error_message: str = "",
) -> None:
    if not session:
        return
    try:
        await session.execute(
            text(
                """
                INSERT INTO rea_api_logs (
                    id, lead_id, rea_upload_id, rea_listing_id, action,
                    request_method, request_path, request_payload,
                    response_status_code, response_body, ok, error_message, created_at
                ) VALUES (
                    :id, :lead_id, :rea_upload_id, :rea_listing_id, :action,
                    :request_method, :request_path, :request_payload,
                    :response_status_code, :response_body, :ok, :error_message, :created_at
                )
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "lead_id": lead_id or "",
                "rea_upload_id": upload_id or "",
                "rea_listing_id": listing_id or "",
                "action": action,
                "request_method": request_method,
                "request_path": request_path,
                "request_payload": json.dumps(request_payload or {}, default=str),
                "response_status_code": response_status_code,
                "response_body": json.dumps(response_body or {}, default=str),
                "ok": 1 if ok else 0,
                "error_message": (error_message or "")[:500],
                "created_at": _now_iso(),
            },
        )
        await session.commit()
    except Exception:
        pass


async def _get_access_token() -> Optional[str]:
    global _access_token, _token_expiry

    if not REA_CLIENT_ID or not REA_CLIENT_SECRET:
        logger.warning("REA credentials missing")
        return None

    now = datetime.datetime.utcnow()
    if _access_token and _token_expiry and now < _token_expiry:
        return _access_token

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                REA_TOKEN_URL,
                data={"grant_type": "client_credentials"},
                auth=(REA_CLIENT_ID, REA_CLIENT_SECRET),
                headers={"Accept": "application/json"},
            )
        if resp.status_code != 200:
            logger.error("REA token failed: %s %s", resp.status_code, resp.text[:200])
            return None
        payload = resp.json()
        _access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        _token_expiry = now + datetime.timedelta(seconds=expires_in - 60)
        return _access_token
    except Exception as exc:
        logger.error("REA token error: %s", exc)
        return None


def _auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _extract_xml_error_details(body: str) -> Dict[str, str]:
    text_body = (body or "").strip()
    if not text_body:
        return {"message": "", "request_id": ""}
    request_id = ""
    message = ""
    try:
        root = ET.fromstring(text_body)
        request_id = _node_text(root, "requestid")
        message = _node_text(root, "message")
    except Exception:
        pass
    if not request_id:
        match = re.search(r"<requestid>([^<]+)</requestid>", text_body, flags=re.IGNORECASE)
        if match:
            request_id = match.group(1).strip()
    if not message:
        match = re.search(r"<message>([^<]+)</message>", text_body, flags=re.IGNORECASE)
        if match:
            message = match.group(1).strip()
    return {"message": message, "request_id": request_id}


def _parse_au_address(full_address: str) -> Dict[str, str]:
    raw = (full_address or "").strip()
    if not raw:
        return {"address": "", "suburb": "", "postcode": ""}
    match = re.match(
        r"^\s*(?P<address>.+?),\s*(?P<suburb>[A-Za-z '\-]+?)\s+NSW\s+(?P<postcode>\d{4})\s*$",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        return {
            "address": (match.group("address") or "").strip(),
            "suburb": (match.group("suburb") or "").strip(),
            "postcode": (match.group("postcode") or "").strip(),
        }
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    if len(parts) >= 2:
        tail = parts[-1]
        postcode_match = re.search(r"\b(\d{4})\b", tail)
        suburb = re.sub(r"\bNSW\b", "", tail, flags=re.IGNORECASE).strip()
        suburb = re.sub(r"\b\d{4}\b", "", suburb).strip()
        return {
            "address": ", ".join(parts[:-1]).strip(),
            "suburb": suburb,
            "postcode": postcode_match.group(1) if postcode_match else "",
        }
    return {"address": raw, "suburb": "", "postcode": ""}


def _seller_leads_to_listing_rows(seller_leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    for lead in seller_leads or []:
        property_data = lead.get("property") or {}
        address_bits = _parse_au_address(str(property_data.get("address") or lead.get("address") or ""))
        if not address_bits.get("address"):
            continue
        dedupe_key = f"{address_bits['address'].strip().lower()}|{address_bits['suburb'].strip().lower()}|{address_bits['postcode'].strip()}"
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        external_id = str(lead.get("id") or "").strip()
        owner_type = str(lead.get("propertyPrimaryUse") or "").strip().lower()
        property_type = "land" if "land" in owner_type else "residential"
        rows.append(
            {
                "address": address_bits["address"],
                "suburb": address_bits["suburb"],
                "postcode": address_bits["postcode"],
                "agency": "The Property Domain",
                "agent": "",
                "est_value": None,
                "bedrooms": None,
                "bathrooms": None,
                "car_spaces": None,
                "land_size_sqm": None,
                "main_image": "",
                "property_images": "[]",
                "rea_listing_id": "",
                "listing_headline": address_bits["address"],
                "listing_description": str(lead.get("comments") or "").strip(),
                "property_type": property_type,
                "signal_status": "OFFMARKET",
                "last_listing_status": "offmarket",
                "rea_status": "offmarket",
                "rea_listing_type": property_type,
                "trigger_type": "rea_seller_lead",
                "source": "rea_partner_seller_leads_fallback",
                "external_reference": external_id,
            }
        )
    return rows


def _build_reaxml(lead: Dict[str, Any], agency_id: str) -> str:
    addr_raw = str(lead.get("address", "")).strip()
    parts = addr_raw.split(" ", 1)
    street_no = parts[0] if len(parts) > 1 and parts[0].rstrip("abcABC").isdigit() else ""
    street_name = parts[1] if street_no else addr_raw

    images = lead.get("property_images")
    if isinstance(images, str):
        try:
            images = json.loads(images)
        except Exception:
            images = []
    images = images or []
    if not images and lead.get("main_image"):
        images = [lead["main_image"]]

    price_low = lead.get("price_guide_low") or lead.get("estimated_value_low")
    price_high = lead.get("price_guide_high") or lead.get("estimated_value_high")
    price_display = ""
    if price_low and price_high:
        lo = int(float(price_low))
        hi = int(float(price_high))
        price_display = f"${lo:,} - ${hi:,}"

    now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H:%M:%S")
    headline = lead.get("listing_headline") or f"For Sale - {addr_raw}"
    desc = lead.get("listing_description") or ""
    if desc and LISTING_COPY_DISCLAIMER not in desc:
        desc = f"{desc}\n\n{LISTING_COPY_DISCLAIMER}"
    agent_name = lead.get("agent") or lead.get("agent_name") or "Nitin Puri"

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<propertyList date="{now_str}" username="{agency_id}">',
        '  <residential status="current">',
        f"    <agentID>{agency_id}</agentID>",
        f"    <uniqueID>{lead.get('id', '')}</uniqueID>",
        f"    <category>{lead.get('property_type') or 'Land'}</category>",
        '    <address display="yes">',
        f"      <streetNumber>{street_no}</streetNumber>",
        f"      <street>{street_name}</street>",
        f"      <suburb>{lead.get('suburb', '')}</suburb>",
        "      <state>NSW</state>",
        f"      <postcode>{lead.get('postcode', '')}</postcode>",
        "      <country>Australia</country>",
        "    </address>",
    ]

    if price_display:
        lines += [
            f'    <price display="yes">{int(float(price_low))}</price>',
            f"    <priceView>{price_display}</priceView>",
        ]

    if lead.get("bedrooms") is not None:
        lines.append(f"    <features><bedrooms>{int(float(lead['bedrooms']))}</bedrooms>")
        if lead.get("bathrooms") is not None:
            lines.append(f"      <bathrooms>{int(float(lead['bathrooms']))}</bathrooms>")
        if lead.get("car_spaces") is not None:
            lines.append(f"      <garages>{int(float(lead['car_spaces']))}</garages>")
        if lead.get("land_size_sqm") is not None:
            lines.append(f'      <landArea unit="squareMeter">{float(lead["land_size_sqm"])}</landArea>')
        lines.append("    </features>")

    lines += [
        f"    <headline><![CDATA[{headline}]]></headline>",
        f"    <description><![CDATA[{desc}]]></description>",
        "    <listingAgent>",
        f"      <name>{agent_name}</name>",
        "      <email>info@thepropertydomain.com.au</email>",
        '      <telephone type="BH">0430042041</telephone>',
        "    </listingAgent>",
    ]

    for i, url in enumerate(images[:20]):
        lines.append(f'    <objects><img id="{i+1}" url="{url}" format="jpeg" modTime="{now_str}" /></objects>')

    lines += ["  </residential>", "</propertyList>"]
    return "\n".join(lines)


async def publish_listing(
    lead: Dict[str, Any],
    agency_id: str = "",
    *,
    session: Optional[AsyncSession] = None,
    lead_id: str = "",
) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured"}

    agency = agency_id or REA_AGENCY_ID
    if not agency:
        return {"ok": False, "error": "REA_AGENCY_ID not set"}
    if len(agency) != 6 or not agency.isalpha():
        return {"ok": False, "error": f"REA_AGENCY_ID must be exactly 6 letters (got '{agency}')"}

    xml_body = _build_reaxml(lead, agency.upper())
    endpoint = "/listing/v1/upload"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                f"{REA_API_BASE}{endpoint}",
                headers={**_auth_headers(token), "Content-Type": "application/xml"},
                content=xml_body.encode("utf-8"),
            )
        if resp.status_code in (200, 201, 202):
            data = resp.json()
            upload_id = data.get("uploadId") or data.get("id") or ""
            result = {
                "ok": True,
                "upload_id": upload_id,
                "status": data.get("status", "submitted"),
                "response": data,
                "rea_listing_id": data.get("listingId") or data.get("listing_id") or "",
            }
            await _log_rea_api_call(
                session,
                lead_id=lead_id or str(lead.get("id") or ""),
                upload_id=upload_id,
                listing_id=result["rea_listing_id"],
                action="publish_listing",
                request_method="POST",
                request_path=endpoint,
                request_payload={"agency_id": agency, "address": lead.get("address", "")},
                response_status_code=resp.status_code,
                response_body=data,
                ok=True,
            )
            return result
        result = {"ok": False, "error": resp.text[:300], "status_code": resp.status_code}
        await _log_rea_api_call(
            session,
            lead_id=lead_id or str(lead.get("id") or ""),
            action="publish_listing",
            request_method="POST",
            request_path=endpoint,
            request_payload={"agency_id": agency, "address": lead.get("address", "")},
            response_status_code=resp.status_code,
            response_body={"text": resp.text[:300]},
            ok=False,
            error_message=result["error"],
        )
        return result
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def get_seller_leads(since_iso: str = "2026-01-01T00:00:00.0Z") -> List[Dict[str, Any]]:
    token = await _get_access_token()
    if not token:
        return []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{REA_API_BASE}/lead/v2/seller-leads",
                headers=_auth_headers(token),
                params={"since": since_iso},
            )
        if resp.status_code == 200:
            return resp.json().get("sellerLeads", [])
        return []
    except Exception:
        return []


async def update_listing(
    rea_listing_id: str,
    updates: Dict[str, Any],
    *,
    session: Optional[AsyncSession] = None,
    lead_id: str = "",
) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured"}
    endpoint = f"/listings/residential/{rea_listing_id}"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.put(f"{REA_API_BASE}{endpoint}", headers=_auth_headers(token), json=updates)
        if resp.status_code in (200, 202, 204):
            await _log_rea_api_call(
                session,
                lead_id=lead_id,
                listing_id=rea_listing_id,
                action="update_listing",
                request_method="PUT",
                request_path=endpoint,
                request_payload=updates,
                response_status_code=resp.status_code,
                response_body={"text": resp.text[:300]},
                ok=True,
            )
            return {"ok": True, "rea_listing_id": rea_listing_id}
        return {"ok": False, "error": resp.text[:300], "status_code": resp.status_code}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def withdraw_listing(
    rea_listing_id: str,
    reason: str = "sold",
    *,
    session: Optional[AsyncSession] = None,
    lead_id: str = "",
) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured"}
    endpoint = f"/listings/residential/{rea_listing_id}/withdraw"
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.put(f"{REA_API_BASE}{endpoint}", headers=_auth_headers(token), json={"reason": reason})
        if resp.status_code in (200, 202, 204):
            await _log_rea_api_call(
                session,
                lead_id=lead_id,
                listing_id=rea_listing_id,
                action="withdraw_listing",
                request_method="PUT",
                request_path=endpoint,
                request_payload={"reason": reason},
                response_status_code=resp.status_code,
                response_body={"text": resp.text[:300]},
                ok=True,
            )
            return {"ok": True, "rea_listing_id": rea_listing_id, "reason": reason}
        return {"ok": False, "error": resp.text[:300], "status_code": resp.status_code}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def get_listing_status(rea_listing_id: str) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{REA_API_BASE}/listings/residential/{rea_listing_id}",
                headers=_auth_headers(token),
            )
        if resp.status_code == 200:
            return {"ok": True, "data": resp.json()}
        return {"ok": False, "error": resp.text[:300], "status_code": resp.status_code}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def sync_agency_feed(
    agency_id: str = "",
    status: str = "current,offmarket,sold",
    listing_types: str = "land,residential",
) -> List[Dict[str, Any]]:
    result = await sync_agency_feed_detailed(
        agency_id=agency_id,
        status=status,
        listing_types=listing_types,
        allow_seller_fallback=False,
    )
    return result.get("listings", []) if result.get("ok") else []


async def sync_agency_feed_detailed(
    agency_id: str = "",
    status: str = "current,offmarket,sold",
    listing_types: str = "land,residential",
    *,
    allow_seller_fallback: bool = True,
    session: Optional[AsyncSession] = None,
) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured", "status_code": 401}
    agency = agency_id or REA_AGENCY_ID
    if not agency:
        return {"ok": False, "error": "REA_AGENCY_ID not set", "status_code": 400}
    endpoint = "/listing/v1/export"
    params = {"agency_id": agency, "status": status, "listing_types": listing_types}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{REA_API_BASE}{endpoint}",
                headers={**_auth_headers(token), "Accept": "application/xml"},
                params=params,
            )
        if resp.status_code == 200:
            listings = _normalize_rea_export_xml(resp.text)
            await _log_rea_api_call(
                session,
                action="sync_agency_feed",
                request_method="GET",
                request_path=endpoint,
                request_payload=params,
                response_status_code=resp.status_code,
                response_body={"count": len(listings)},
                ok=True,
            )
            return {
                "ok": True,
                "source": "listing_export",
                "degraded": False,
                "listings": listings,
                "total_from_rea": len(listings),
                "status_code": resp.status_code,
            }

        details = _extract_xml_error_details(resp.text)
        export_error = {
            "status_code": resp.status_code,
            "message": details.get("message") or resp.text[:300],
            "request_id": details.get("request_id") or "",
            "endpoint": endpoint,
            "params": params,
        }
        await _log_rea_api_call(
            session,
            action="sync_agency_feed",
            request_method="GET",
            request_path=endpoint,
            request_payload=params,
            response_status_code=resp.status_code,
            response_body={"text": resp.text[:300], "export_error": export_error},
            ok=False,
            error_message=export_error["message"],
        )

        if allow_seller_fallback:
            seller_leads = await get_seller_leads("2026-01-01T00:00:00.0Z")
            fallback_rows = _seller_leads_to_listing_rows(seller_leads)
            if fallback_rows:
                return {
                    "ok": True,
                    "source": "seller_leads_fallback",
                    "degraded": True,
                    "warning": "REA listing export failed; using seller-leads fallback",
                    "export_error": export_error,
                    "listings": fallback_rows,
                    "total_from_rea": len(fallback_rows),
                    "status_code": resp.status_code,
                }

        return {"ok": False, "error": "REA listing export failed", "export_error": export_error, "status_code": resp.status_code}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "status_code": 500}


def _normalize_rea_listing(item: Dict[str, Any]) -> Dict[str, Any]:
    addr = item.get("address") or {}
    price = item.get("price") or {}
    features = item.get("features") or {}
    media = item.get("media") or []
    photos = [m.get("url") for m in media if m.get("type") == "photo" and m.get("url")]
    listers = item.get("listers") or []
    agent_name = listers[0].get("name") if listers else ""

    est_value = None
    if price.get("from") and price.get("to"):
        est_value = int((price["from"] + price["to"]) / 2)
    elif price.get("display"):
        try:
            est_value = int("".join(c for c in price["display"] if c.isdigit()))
        except Exception:
            pass

    return {
        "address": addr.get("displayAddress") or addr.get("streetAddress", ""),
        "suburb": addr.get("suburb", ""),
        "postcode": addr.get("postcode", ""),
        "agency": "The Property Domain",
        "agent": agent_name,
        "est_value": est_value,
        "bedrooms": features.get("bedrooms"),
        "bathrooms": features.get("bathrooms"),
        "car_spaces": features.get("carSpaces"),
        "land_size_sqm": (features.get("landArea") or {}).get("value"),
        "main_image": photos[0] if photos else None,
        "property_images": json.dumps(photos) if photos else "[]",
        "rea_listing_id": item.get("listingId") or item.get("id", ""),
        "listing_headline": item.get("headline", ""),
        "signal_status": "LIVE",
        "trigger_type": "rea_feed",
        "source": "rea_partner_api",
    }


def _node_text(node: Optional[ET.Element], path: str, default: str = "") -> str:
    if node is None:
        return default
    child = node.find(path)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _safe_float(text_value: str) -> Optional[float]:
    try:
        return float(text_value.strip())
    except Exception:
        return None


def _safe_int(text_value: str) -> Optional[int]:
    try:
        return int(float(text_value.strip()))
    except Exception:
        return None


def _normalize_rea_export_xml(xml_body: str) -> List[Dict[str, Any]]:
    if not (xml_body or "").strip():
        return []
    try:
        root = ET.fromstring(xml_body)
    except Exception:
        return []

    items: List[Dict[str, Any]] = []
    for category_tag in ("land", "residential", "rental", "commercial"):
        for node in root.findall(f".//{category_tag}"):
            status = (node.attrib.get("status") or "").strip().lower()
            listing_id = _node_text(node, "listingId")
            unique_id = _node_text(node, "uniqueID")
            address_node = node.find("address")
            lot_number = _node_text(address_node, "lotNumber")
            street_no = _node_text(address_node, "streetNumber")
            street = _node_text(address_node, "street")
            suburb = _node_text(address_node, "suburb")
            postcode = _node_text(address_node, "postcode")
            addr_parts = []
            if lot_number:
                addr_parts.append(f"Lot {lot_number}")
            if street_no:
                addr_parts.append(street_no)
            if street:
                addr_parts.append(street)
            display_address = " ".join(addr_parts).strip()
            if not display_address:
                display_address = _node_text(address_node, "display")
            if not display_address:
                display_address = _node_text(address_node, "fullAddress")

            headline = _node_text(node, "headline")
            description = _node_text(node, "description")
            category_text = (_node_text(node, "category", default=category_tag) or category_tag).strip()
            listing_type = category_text.lower() or category_tag

            features_node = node.find("features")
            bedrooms = _safe_int(_node_text(features_node, "bedrooms"))
            bathrooms = _safe_int(_node_text(features_node, "bathrooms"))
            car_spaces = _safe_int(_node_text(features_node, "garages"))
            land_size = _safe_float(_node_text(features_node, "landArea"))
            # Land listings store area in <landDetails><area> instead of features
            if land_size is None:
                land_details_node = node.find("landDetails")
                if land_details_node is not None:
                    area_node = land_details_node.find("area")
                    if area_node is not None and area_node.text:
                        land_size = _safe_float(area_node.text)

            img_urls: List[str] = []
            for img in node.findall(".//objects/img"):
                url = (img.attrib.get("url") or "").strip()
                if url:
                    img_urls.append(url)
            for img in node.findall(".//images/image"):
                url = _node_text(img, "url")
                if url:
                    img_urls.append(url)
            # dedupe while preserving order
            seen: set[str] = set()
            image_urls: List[str] = []
            for url in img_urls:
                if url not in seen:
                    seen.add(url)
                    image_urls.append(url)

            agent_name = _node_text(node, "listingAgent/name") or _node_text(node, "agent/name")
            price_view = _node_text(node, "priceView")
            price_value = _safe_int(_node_text(node, "price"))
            est_value = price_value
            if est_value is None and price_view:
                numeric = "".join(ch for ch in price_view if ch.isdigit())
                if numeric:
                    est_value = _safe_int(numeric)

            items.append(
                {
                    "address": display_address,
                    "suburb": suburb,
                    "postcode": postcode,
                    "agency": "The Property Domain",
                    "agent": agent_name,
                    "est_value": est_value,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "car_spaces": car_spaces,
                    "land_size_sqm": land_size,
                    "main_image": image_urls[0] if image_urls else None,
                    "property_images": json.dumps(image_urls) if image_urls else "[]",
                    "rea_listing_id": listing_id or unique_id,
                    "listing_headline": headline,
                    "listing_description": description,
                    "property_type": category_text,
                    "signal_status": status.upper() if status else "LIVE",
                    "last_listing_status": status or "current",
                    "rea_status": status or "current",
                    "rea_listing_type": listing_type,
                    "trigger_type": "rea_feed",
                    "source": "rea_partner_api",
                }
            )
    return items


async def check_credentials() -> Dict[str, Any]:
    if not REA_CLIENT_ID or not REA_CLIENT_SECRET:
        return {"configured": False, "message": "REA_CLIENT_ID or REA_CLIENT_SECRET not set in .env"}
    token = await _get_access_token()
    if token:
        return {
            "configured": True,
            "token_ok": True,
            "agency_id": REA_AGENCY_ID or "not set - add REA_AGENCY_ID to .env",
            "message": "REA credentials valid",
        }
    return {
        "configured": True,
        "token_ok": False,
        "message": "Credentials set but token fetch failed - check REA_CLIENT_ID / REA_CLIENT_SECRET",
    }


async def get_export_diagnostics(agency_id: str = "") -> Dict[str, Any]:
    cred = await check_credentials()
    diagnostics: Dict[str, Any] = {
        "credentials": cred,
        "integration_scope_ok": False,
        "export_probe": {},
        "seller_leads_probe": {},
    }
    token = await _get_access_token()
    if not token:
        diagnostics["export_probe"] = {"ok": False, "error": "token_unavailable"}
        diagnostics["seller_leads_probe"] = {"ok": False, "error": "token_unavailable"}
        return diagnostics

    required_owner = (agency_id or REA_AGENCY_ID or "").strip().upper()
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            integrations_resp = await client.get(
                f"{REA_API_BASE}/me/v1/integrations",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            )
        if integrations_resp.status_code == 200:
            payload = integrations_resp.json()
            integrations = ((payload.get("_embedded") or {}).get("integrations") or [])
            for item in integrations:
                owner_id = str(item.get("ownerId") or "").strip().upper()
                scopes = item.get("scopes") or []
                if owner_id == required_owner and "listing:listings:export" in scopes:
                    diagnostics["integration_scope_ok"] = True
                    break
    except Exception:
        pass

    export_probe = await sync_agency_feed_detailed(
        agency_id=agency_id,
        status="current,offmarket,sold",
        listing_types="land,residential",
        allow_seller_fallback=False,
        session=None,
    )
    diagnostics["export_probe"] = {
        "ok": bool(export_probe.get("ok")),
        "status_code": export_probe.get("status_code"),
        "source": export_probe.get("source", ""),
        "total": int(export_probe.get("total_from_rea") or 0),
        "export_error": export_probe.get("export_error") or {},
        "error": export_probe.get("error", ""),
    }
    seller = await get_seller_leads("2026-01-01T00:00:00.0Z")
    diagnostics["seller_leads_probe"] = {"ok": True, "count": len(seller or [])}
    return diagnostics


async def export_listings(session: Optional[AsyncSession] = None) -> List[Dict[str, Any]]:
    if not session:
        return []
    rows = (
        await session.execute(
            text(
                """
                SELECT id, address, suburb, postcode, owner_name, property_type,
                       listing_headline, rea_listing_id, rea_upload_id, rea_upload_status,
                       updated_at, created_at
                FROM leads
                WHERE COALESCE(rea_listing_id, '') <> '' OR COALESCE(rea_upload_id, '') <> ''
                ORDER BY COALESCE(updated_at, created_at, '') DESC
                LIMIT 1000
                """
            )
        )
    ).mappings().all()
    return [dict(row) for row in rows]


async def get_upload_report(
    upload_id: str,
    *,
    session: Optional[AsyncSession] = None,
    lead_id: str = "",
) -> Dict[str, Any]:
    token = await _get_access_token()
    if not token:
        return {"ok": False, "error": "REA credentials not configured"}

    endpoint = f"/listing/v1/upload/{upload_id}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{REA_API_BASE}{endpoint}", headers=_auth_headers(token))
        if resp.status_code == 200:
            data = resp.json()
            return {"ok": True, "data": data}

        fallback: Dict[str, Any] = {"uploadId": upload_id, "progress": "processing"}
        if session:
            local = (
                await session.execute(
                    text(
                        """
                        SELECT rea_upload_status, rea_listing_id, rea_last_upload_report
                        FROM leads
                        WHERE rea_upload_id = :upload_id
                        LIMIT 1
                        """
                    ),
                    {"upload_id": upload_id},
                )
            ).mappings().first()
            if local:
                fallback["progress"] = local.get("rea_upload_status") or "processing"
                if local.get("rea_listing_id"):
                    fallback["listingId"] = local.get("rea_listing_id")
                try:
                    parsed = json.loads(local.get("rea_last_upload_report") or "{}")
                    if isinstance(parsed, dict):
                        fallback.update({k: v for k, v in parsed.items() if k not in fallback})
                except Exception:
                    pass
        await _log_rea_api_call(
            session,
            lead_id=lead_id,
            upload_id=upload_id,
            action="upload_report",
            request_method="GET",
            request_path=endpoint,
            response_status_code=resp.status_code,
            response_body={"text": resp.text[:300], "fallback": fallback},
            ok=False,
            error_message=resp.text[:300],
        )
        return {"ok": True, "data": fallback}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


async def get_integration_status(session: Optional[AsyncSession] = None) -> Dict[str, Any]:
    cred = await check_credentials()
    payload: Dict[str, Any] = {
        "credentials": cred,
        "configured": bool(cred.get("configured")),
        "token_ok": bool(cred.get("token_ok")),
        "stats": {},
        "recent_logs": [],
    }
    if not session:
        return payload

    stats = (
        await session.execute(
            text(
                """
                SELECT
                    COUNT(*) FILTER (WHERE COALESCE(rea_upload_id, '') <> '') AS total_uploaded,
                    COUNT(*) FILTER (WHERE COALESCE(rea_listing_id, '') <> '') AS total_live_linked,
                    COUNT(*) FILTER (WHERE COALESCE(rea_upload_status, '') IN ('failed', 'rejected', 'error')) AS total_failed
                FROM leads
                """
            )
        )
    ).mappings().first()
    logs = (
        await session.execute(
            text(
                """
                SELECT action, ok, response_status_code, created_at
                FROM rea_api_logs
                ORDER BY created_at DESC
                LIMIT 20
                """
            )
        )
    ).mappings().all()
    payload["stats"] = dict(stats or {})
    payload["recent_logs"] = [dict(row) for row in logs]
    return payload


async def get_enquiries(since: str = "", session: Optional[AsyncSession] = None) -> List[Dict[str, Any]]:
    leads = await get_seller_leads(since or "2026-01-01T00:00:00.0Z")
    if leads:
        return leads
    if not session:
        return []
    rows = (
        await session.execute(
            text(
                """
                SELECT id, owner_name, address, suburb, postcode, created_at, updated_at
                FROM leads
                WHERE COALESCE(trigger_type, '') = 'rea_seller_lead'
                ORDER BY COALESCE(updated_at, created_at, '') DESC
                LIMIT 200
                """
            )
        )
    ).mappings().all()
    return [dict(row) for row in rows]


async def get_listing_performance(listing_id: str, session: Optional[AsyncSession] = None) -> Dict[str, Any]:
    token = await _get_access_token()
    if token:
        endpoint = f"/campaign/v1/listing-performance/{listing_id}"
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{REA_API_BASE}{endpoint}", headers=_auth_headers(token))
            if resp.status_code == 200:
                return {"ok": True, "source": "rea_api", "listing_id": listing_id, "metrics": resp.json()}
        except Exception:
            pass

    if not session:
        return {"ok": False, "error": "Performance endpoint unavailable and no session fallback"}

    row = (
        await session.execute(
            text(
                """
                SELECT listing_id, views_a, views_b, enquiries_a, enquiries_b, ctr_a, ctr_b, status
                FROM rea_ab_tests
                WHERE listing_id = :listing_id
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {"listing_id": listing_id},
        )
    ).mappings().first()
    if not row:
        return {"ok": True, "source": "local_fallback", "listing_id": listing_id, "metrics": {}}
    return {"ok": True, "source": "local_ab_tests", "listing_id": listing_id, "metrics": dict(row)}
