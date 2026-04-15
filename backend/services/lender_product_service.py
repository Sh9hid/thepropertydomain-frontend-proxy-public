from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from models.sql_models import BankDataHolder, LenderProduct, LenderProductDelta, LenderProductSnapshot
from services.cdr_lenders import get_cdr_bank_registry

PRIORITY_LENDERS = ("CommBank", "CBA - CommBiz", "NATIONAL AUSTRALIA BANK", "ANZ", "Westpac", "St.George Bank")


def _now_iso() -> str:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _first_dict_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _walk(node: Any) -> Iterable[Any]:
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _extract_products(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = [
        payload.get("data", {}).get("products") if isinstance(payload.get("data"), dict) else None,
        payload.get("products"),
        payload.get("data") if isinstance(payload.get("data"), list) else None,
    ]
    for candidate in candidates:
        rows = _first_dict_list(candidate)
        if rows:
            return rows
    return []


def _find_first_numeric(node: Any, keys: Iterable[str]) -> Optional[float]:
    wanted = {item.lower() for item in keys}
    for branch in _walk(node):
        if not isinstance(branch, dict):
            continue
        for key, value in branch.items():
            if str(key).lower() in wanted:
                parsed = _safe_float(value)
                if parsed is not None:
                    return parsed
    return None


def _find_tags(text: str) -> List[str]:
    lower = text.lower()
    tags: List[str] = []
    if "offset" in lower:
        tags.append("offset")
    if "redraw" in lower:
        tags.append("redraw")
    if "interest only" in lower:
        tags.append("interest_only")
    if "package" in lower:
        tags.append("package")
    if "cashback" in lower:
        tags.append("cashback")
    return tags


def _parse_fixed_term_months(text: str) -> Optional[int]:
    match = re.search(r"(\d+)\s*(year|yr|years|yrs|month|months|mo)", text.lower())
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    return amount * 12 if unit.startswith("y") else amount


def _best_rate_from_product(product: Dict[str, Any]) -> tuple[Optional[float], str]:
    lending_rates = _first_dict_list(product.get("lendingRates"))
    best_rate: Optional[float] = None
    best_type = "unknown"
    for item in lending_rates:
        rate = _safe_float(item.get("rate"))
        if rate is None:
            continue
        rate_type = str(item.get("lendingRateType") or item.get("type") or "").lower()
        if best_rate is None or rate < best_rate:
            best_rate = rate
            if "fixed" in rate_type:
                best_type = "fixed"
            elif "variable" in rate_type or "introductory" in rate_type:
                best_type = "variable"
            else:
                best_type = rate_type or "unknown"
    if best_rate is not None:
        return best_rate, best_type
    text_blob = " ".join(str(product.get(key) or "") for key in ("name", "description", "brand"))
    return _find_first_numeric(product, {"rate", "interestRate"}), ("fixed" if "fixed" in text_blob.lower() else "variable" if "variable" in text_blob.lower() else "unknown")


def _comparison_rate_from_product(product: Dict[str, Any]) -> Optional[float]:
    lending_rates = _first_dict_list(product.get("lendingRates"))
    for item in lending_rates:
        comparison = _safe_float(item.get("comparisonRate"))
        if comparison is not None:
            return comparison
    return _find_first_numeric(product, {"comparisonRate", "comparison_rate"})


def normalize_products_for_lender(lender: BankDataHolder, payload: Dict[str, Any], fetched_at: Optional[str] = None) -> List[Dict[str, Any]]:
    fetched_at = fetched_at or _now_iso()
    records: List[Dict[str, Any]] = []
    for product in _extract_products(payload):
        name = str(product.get("name") or product.get("productName") or product.get("brandName") or "Unnamed product").strip()
        description = str(product.get("description") or "")
        text_blob = " ".join([name, description, str(product.get("brand") or lender.brand or lender.name)])
        advertised_rate, rate_type = _best_rate_from_product(product)
        comparison_rate = _comparison_rate_from_product(product)
        if advertised_rate is None and comparison_rate is None:
            continue
        tags = _find_tags(text_blob)
        occupancy_target = "investor" if "investor" in text_blob.lower() else "owner_occupier" if any(token in text_blob.lower() for token in ("owner occupier", "owner-occupier", "owner occupied")) else "unknown"
        fixed_term_months = _parse_fixed_term_months(text_blob) if rate_type == "fixed" else None
        package_fee = _find_first_numeric(product, {"annualFee", "packageFee", "fee"})
        records.append(
            {
                "id": f"{lender.id}:{product.get('productId') or product.get('id') or uuid.uuid4().hex}",
                "lender_id": lender.id,
                "external_product_id": str(product.get("productId") or product.get("id") or uuid.uuid4().hex),
                "name": name,
                "brand": lender.brand or lender.name,
                "product_kind": "mortgage",
                "occupancy_target": occupancy_target,
                "rate_type": rate_type,
                "advertised_rate": advertised_rate,
                "comparison_rate": comparison_rate,
                "fixed_term_months": fixed_term_months,
                "has_offset": "offset" in tags,
                "has_redraw": "redraw" in tags,
                "interest_only_available": "interest_only" in tags or "interest only" in text_blob.lower(),
                "package_fee_annual": package_fee,
                "tags_json": tags,
                "constraints_json": {
                    "raw_name": name,
                    "description_present": bool(description),
                    "product_category": product.get("productCategory"),
                },
                "raw_json": product,
                "source_url": f"{lender.base_url.rstrip('/')}{lender.product_path}",
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    return records


async def fetch_lender_payload(lender: BankDataHolder, client: Optional[httpx.AsyncClient] = None) -> tuple[int, Dict[str, Any]]:
    url = f"{lender.base_url.rstrip('/')}{lender.product_path}"
    headers = {
        "Accept": "application/json",
        "x-v": "1",
    }
    if client is not None:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.status_code, response.json()
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as local_client:
        response = await local_client.get(url, headers=headers)
        response.raise_for_status()
        return response.status_code, response.json()


async def _ensure_registry_seeded(session: AsyncSession) -> None:
    existing = set((await session.execute(select(BankDataHolder.id))).scalars().all())
    now_iso = _now_iso()
    for record in get_cdr_bank_registry():
        if str(record["id"]) in existing:
            continue
        session.add(BankDataHolder(created_at=now_iso, updated_at=now_iso, **record))
    await session.commit()


async def sync_lender_products(
    session: AsyncSession,
    lender_id: Optional[str] = None,
    *,
    client: Optional[httpx.AsyncClient] = None,
) -> Dict[str, Any]:
    await _ensure_registry_seeded(session)
    stmt = select(BankDataHolder).where(BankDataHolder.active == True)  # noqa: E712
    if lender_id:
        stmt = stmt.where(BankDataHolder.id == lender_id)
    lenders = list((await session.execute(stmt)).scalars().all())
    if lender_id and not lenders:
        return {"processed": 0, "updated_products": 0, "failed": 0, "lenders": []}

    processed = 0
    updated_products = 0
    failed = 0
    results: List[Dict[str, Any]] = []

    for lender in lenders:
        processed += 1
        fetched_at = _now_iso()
        source_url = f"{lender.base_url.rstrip('/')}{lender.product_path}"
        try:
            status_code, payload = await fetch_lender_payload(lender, client=client)
            snapshot = LenderProductSnapshot(
                id=str(uuid.uuid4()),
                lender_id=lender.id,
                source_url=source_url,
                status="success",
                http_status=status_code,
                response_json=payload,
                fetched_at=fetched_at,
            )
            session.add(snapshot)
            normalized = normalize_products_for_lender(lender, payload, fetched_at=fetched_at)
            existing_rows = list((await session.execute(select(LenderProduct).where(LenderProduct.lender_id == lender.id))).scalars().all())
            existing_by_external = {row.external_product_id: row for row in existing_rows}
            next_by_external = {str(row["external_product_id"]): row for row in normalized}
            for external_id, next_row in next_by_external.items():
                current = existing_by_external.get(external_id)
                if current is None:
                    session.add(
                        LenderProductDelta(
                            id=str(uuid.uuid4()),
                            lender_id=lender.id,
                            external_product_id=external_id,
                            change_type="product_added",
                            headline=f"{lender.name} added {next_row['name']}",
                            new_rate=next_row.get("advertised_rate"),
                            new_comparison_rate=next_row.get("comparison_rate"),
                            payload_json={"new": next_row},
                            detected_at=fetched_at,
                        )
                    )
                    continue
                old_rate = current.advertised_rate
                new_rate = next_row.get("advertised_rate")
                old_comp = current.comparison_rate
                new_comp = next_row.get("comparison_rate")
                if old_rate != new_rate or old_comp != new_comp:
                    session.add(
                        LenderProductDelta(
                            id=str(uuid.uuid4()),
                            lender_id=lender.id,
                            external_product_id=external_id,
                            change_type="rate_changed",
                            headline=f"{lender.name} changed {next_row['name']}",
                            old_rate=old_rate,
                            new_rate=new_rate,
                            old_comparison_rate=old_comp,
                            new_comparison_rate=new_comp,
                            payload_json={"old": current.raw_json, "new": next_row},
                            detected_at=fetched_at,
                        )
                    )
            for external_id, current in existing_by_external.items():
                if external_id not in next_by_external:
                    session.add(
                        LenderProductDelta(
                            id=str(uuid.uuid4()),
                            lender_id=lender.id,
                            external_product_id=external_id,
                            change_type="product_removed",
                            headline=f"{lender.name} removed {current.name}",
                            old_rate=current.advertised_rate,
                            old_comparison_rate=current.comparison_rate,
                            payload_json={"old": current.raw_json},
                            detected_at=fetched_at,
                        )
                    )
            await session.execute(delete(LenderProduct).where(LenderProduct.lender_id == lender.id))
            for row in normalized:
                session.add(LenderProduct(**row))
            updated_products += len(normalized)
            results.append({"lender_id": lender.id, "name": lender.name, "status": "success", "product_count": len(normalized)})
        except Exception as exc:
            failed += 1
            session.add(
                LenderProductSnapshot(
                    id=str(uuid.uuid4()),
                    lender_id=lender.id,
                    source_url=source_url,
                    status="failed",
                    error_message=str(exc),
                    fetched_at=fetched_at,
                )
            )
            results.append({"lender_id": lender.id, "name": lender.name, "status": "failed", "error": str(exc)})
    await session.commit()
    return {"processed": processed, "updated_products": updated_products, "failed": failed, "lenders": results}


async def list_lender_products(
    session: AsyncSession,
    *,
    occupancy_target: Optional[str] = None,
    rate_type: Optional[str] = None,
    limit: int = 50,
) -> List[LenderProduct]:
    stmt = select(LenderProduct)
    if occupancy_target:
        stmt = stmt.where(LenderProduct.occupancy_target.in_([occupancy_target, "unknown"]))
    if rate_type:
        stmt = stmt.where(LenderProduct.rate_type == rate_type)
    stmt = stmt.order_by(LenderProduct.advertised_rate.asc().nullslast(), LenderProduct.comparison_rate.asc().nullslast()).limit(limit)
    return list((await session.execute(stmt)).scalars().all())


async def best_market_rate(
    session: AsyncSession,
    *,
    occupancy_target: str,
    rate_type: Optional[str] = None,
) -> Dict[str, Any]:
    candidates = await list_lender_products(session, occupancy_target=occupancy_target, rate_type=rate_type, limit=25)
    ranked = [item for item in candidates if item.advertised_rate is not None]
    if not ranked:
        return {"rate": None, "comparison_rate": None, "lender_name": None, "product_name": None}
    best_rate = min(item.advertised_rate for item in ranked if item.advertised_rate is not None)
    close_band = [
        item for item in ranked
        if item.advertised_rate is not None and float(item.advertised_rate) <= float(best_rate) + 0.15
    ]
    def _priority_key(item: LenderProduct) -> tuple[int, float, float]:
        lender_name = str(item.brand or "")
        preferred_rank = next((index for index, label in enumerate(PRIORITY_LENDERS) if label.lower() == lender_name.lower()), 999)
        return (preferred_rank, float(item.advertised_rate or 999.0), float(item.comparison_rate or 999.0))
    preferred = sorted(close_band, key=_priority_key)
    top = preferred[0] if preferred else ranked[0]
    return {
        "rate": top.advertised_rate,
        "comparison_rate": top.comparison_rate,
        "lender_name": top.brand or top.lender_id,
        "product_name": top.name,
        "rate_type": top.rate_type,
        "occupancy_target": top.occupancy_target,
        "priority_bias_applied": True,
    }


async def list_recent_lender_deltas(session: AsyncSession, limit: int = 50) -> List[LenderProductDelta]:
    rows = await session.execute(
        select(LenderProductDelta)
        .order_by(LenderProductDelta.detected_at.desc())
        .limit(limit)
    )
    return list(rows.scalars().all())
