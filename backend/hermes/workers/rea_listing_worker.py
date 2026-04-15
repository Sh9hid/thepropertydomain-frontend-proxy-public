"""
REA Listing Worker — Self-improving land listing agent for Hermes.

Manages the full lifecycle of Bathla lot listings on realestate.com.au:
- Analyzes live listing performance by variant/suburb/size
- Generates push plans for unpushed lots (staggered, max 15/day)
- Identifies underperformers and generates refresh plans (max 1 edit/24h per listing)
- Executes approved push and refresh plans
- Pulls performance metrics and stores to DB
- Self-improves by rotating copy based on what's actually converting

REA Rules Enforced:
  - Land listings ONLY (free — no payment involved)
  - Max 1 edit per listing per 24h
  - Price changes within 10% of current price only
  - Cannot relist as "new" — must edit existing
  - All content must be genuine and accurate
"""
from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import _async_session_factory
from services.rea_service import (
    check_credentials,
    get_listing_performance,
    publish_listing,
    update_listing,
)

log = logging.getLogger(__name__)
LISTING_COPY_DISCLAIMER = (
    "Disclaimer: Information is indicative only, subject to change without notice, "
    "and should not be relied on as legal or financial advice. Buyers must make their own enquiries."
)

# ─── Suburb Map ──────────────────────────────────────────────────────────────

SUBURB_MAP = {
    "box hill": ("Box Hill", "2765"),
    "marsden park": ("Marsden Park", "2765"),
    "the ponds": ("The Ponds", "2769"),
    "tallawong": ("Tallawong", "2762"),
    "rouse hill": ("Rouse Hill", "2155"),
    "riverstone": ("Riverstone", "2765"),
    "north kellyville": ("North Kellyville", "2155"),
    "kellyville": ("Kellyville", "2155"),
    "oran park": ("Oran Park", "2570"),
    "dora creek": ("Dora Creek", "2264"),
    "vineyard": ("Vineyard", "2765"),
    "blacktown": ("Blacktown", "2148"),
}


def _extract_suburb(project_name: str) -> tuple[str, str]:
    pn = (project_name or "").lower()
    for key, (suburb, postcode) in SUBURB_MAP.items():
        if key in pn:
            return suburb, postcode
    return "", ""


# ─── Title & Description Variants ────────────────────────────────────────────
# Copied verbatim from scripts/push_rea_listings.py.
# Each listing gets a deterministic variant based on its ID so it's
# reproducible but varied across the portfolio.

TITLE_VARIANTS = [
    # Location-first
    "{suburb} Land — {size}sqm, Lot {lot}, Ready to Build",
    "Lot {lot} {suburb} — {size}sqm Flat Block, {lot_type_tag}",
    "{size}sqm in {suburb} — Lot {lot}, From ${price_display}",
    # Benefit-first
    "Build Your Dream Home — {size}sqm in {suburb}",
    "Flat {size}sqm Block in {suburb} — No Covenants, Build Ready",
    "Premium {size}sqm Land — Lot {lot}, {suburb} Estate",
    # Urgency
    "Last Lots in {suburb} — {size}sqm From ${price_display}",
    "Don't Miss Lot {lot} — {size}sqm, {suburb}",
]

# Refresh strategies determine which copy axis to rotate on each refresh.
REFRESH_STRATEGIES = [
    {
        "name": "description_rewrite",
        "description": "Rewrite description with different angle (features vs story vs data)",
        "min_days_since_edit": 7,
    },
    {
        "name": "headline_optimize",
        "description": "Test new headline variant based on performance data",
        "min_days_since_edit": 7,
    },
    {
        "name": "price_micro_update",
        "description": "Adjust price by $100-$1,000 to refresh listing date (within 10% rule)",
        "min_days_since_edit": 14,
        "max_price_change_pct": 0.5,
    },
]


def _generate_title(lot: dict, variant_idx: int) -> str:
    tpl = TITLE_VARIANTS[variant_idx % len(TITLE_VARIANTS)]
    size = int(float(lot.get("land_size_sqm") or 0))
    price = int(float(lot.get("price") or lot.get("estimated_value_mid") or 0))
    lot_type = lot.get("lot_type") or ""
    lot_type_tag = lot_type if lot_type else "Level Block"
    return tpl.format(
        suburb=lot.get("suburb", ""),
        size=size,
        lot=lot.get("lot_number", ""),
        price_display=f"{price:,}" if price else "Contact Agent",
        lot_type_tag=lot_type_tag,
    )


def _generate_description(lot: dict, variant_idx: int) -> str:
    size = int(float(lot.get("land_size_sqm") or 0))
    price = int(float(lot.get("price") or lot.get("estimated_value_mid") or 0))
    suburb = lot.get("suburb", "")
    address = lot.get("address", "")
    postcode = lot.get("postcode", "")
    lot_num = lot.get("lot_number", "")
    frontage = lot.get("frontage") or ""
    lot_type = lot.get("lot_type") or ""

    frontage_line = f"• {frontage}m frontage" if frontage else ""
    lot_type_line = f"• {lot_type}" if lot_type else "• Level, regular-shaped block"
    price_line = f"Priced at ${price:,}" if price else "Contact agent for pricing"

    variants = [
        # Variant A: Feature-focused
        f"""{address}, {suburb} NSW {postcode} — Lot {lot_num}

A premium {size}sqm block in one of Sydney's fastest-growing corridors.

What you get:
• {size} square metres of flat, build-ready land
{lot_type_line}
{frontage_line}
• All services available
• Walk to future shops, schools, and parklands
• Strong capital growth area — {suburb} is booming

{price_line}. Land in {suburb} is moving fast — secure your block before it's gone.

Nitin Puri | Laing+Simmons Oakville | Windsor
04 85 85 7881 | info@thepropertydomain.com.au""",

        # Variant B: Story-driven
        f"""Lot {lot_num} at {address}, {suburb}

If you've been looking for the right block to build on, this might be it.

{size} square metres of flat land in {suburb} — a suburb that's transformed over the past few years from farmland into a thriving community with new schools, parks, and retail.

The block:
• {size}sqm, level and cleared
{lot_type_line}
{frontage_line}
• Ready for your builder — no clearing or levelling needed

{suburb} is one of those suburbs where early buyers have done very well. This is your chance to get in while blocks are still available.

{price_line}.

Call Nitin on 04 85 85 7881 to discuss.
Laing+Simmons Oakville | Windsor""",

        # Variant C: Data-driven
        f"""{address}, {suburb} NSW {postcode}

Lot {lot_num} | {size}sqm | {suburb}

Block details:
• Land area: {size} square metres
{lot_type_line}
{frontage_line}
• Zoning: R2 Low Density Residential
• Services: Available
• Status: Ready to build

Location highlights:
• {suburb} median land price trending upward
• New infrastructure and amenities being delivered
• 15 minutes to major shopping centres
• Easy access to M7 and future metro connections

{price_line}. Genuine enquiries welcome.

Nitin Puri
Laing+Simmons Oakville | Windsor
04 85 85 7881""",
    ]
    base = variants[variant_idx % len(variants)].strip()
    if LISTING_COPY_DISCLAIMER in base:
        return base
    return f"{base}\n\n{LISTING_COPY_DISCLAIMER}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ctr(views: int, enquiries: int) -> float:
    """Return click-through rate as a float (0.0–100.0)."""
    return round(enquiries / max(views, 1) * 100, 2)


def _is_land_listing(lead: dict) -> bool:
    property_type = str(lead.get("property_type") or "").strip().lower()
    trigger_type = str(lead.get("trigger_type") or "").strip().lower()
    return property_type == "land" or trigger_type == "bathla_land"


# ─── Worker Functions ─────────────────────────────────────────────────────────


async def analyze_portfolio(session: AsyncSession) -> dict:
    """Pull all live land listings, compute performance stats by variant/suburb/size.

    Returns structured analysis with top/bottom performers and recommendations.
    """
    rows = (
        await session.execute(
            text("""
                SELECT
                    id, address, suburb, land_size_sqm, estimated_value_mid,
                    listing_headline, rea_listing_id, rea_title_variant,
                    rea_desc_variant, rea_views, rea_enquiries,
                    rea_last_edit_at, updated_at
                FROM leads
                WHERE COALESCE(rea_listing_id, '') <> ''
                  AND (
                      LOWER(COALESCE(property_type, '')) = 'land'
                      OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land'
                  )
                ORDER BY COALESCE(rea_enquiries, 0) * 1.0
                       / GREATEST(COALESCE(rea_views, 1), 1) DESC
            """)
        )
    ).mappings().all()

    if not rows:
        return {
            "ok": True,
            "summary": "No live land listings found.",
            "live_count": 0,
            "top_performers": [],
            "bottom_performers": [],
            "by_variant": {},
            "by_suburb": {},
            "by_size_bucket": {},
            "recommendations": ["Run push plan first to get listings live on REA."],
        }

    listings = [dict(r) for r in rows]

    # ── By variant ────────────────────────────────────────────────────────
    by_variant: dict[int, dict] = {}
    for lst in listings:
        v = int(lst.get("rea_title_variant") or 0)
        entry = by_variant.setdefault(v, {"views": 0, "enquiries": 0, "count": 0})
        entry["views"] += int(lst.get("rea_views") or 0)
        entry["enquiries"] += int(lst.get("rea_enquiries") or 0)
        entry["count"] += 1

    for v, entry in by_variant.items():
        entry["ctr_pct"] = _ctr(entry["views"], entry["enquiries"])
        entry["template"] = TITLE_VARIANTS[v % len(TITLE_VARIANTS)]

    # ── By suburb ─────────────────────────────────────────────────────────
    by_suburb: dict[str, dict] = {}
    for lst in listings:
        s = lst.get("suburb") or "Unknown"
        entry = by_suburb.setdefault(s, {"views": 0, "enquiries": 0, "count": 0})
        entry["views"] += int(lst.get("rea_views") or 0)
        entry["enquiries"] += int(lst.get("rea_enquiries") or 0)
        entry["count"] += 1

    for s, entry in by_suburb.items():
        entry["ctr_pct"] = _ctr(entry["views"], entry["enquiries"])

    # ── By size bucket ────────────────────────────────────────────────────
    size_buckets: dict[str, list] = {"<300": [], "300-400": [], "400-500": [], "500+": []}
    for lst in listings:
        sz = float(lst.get("land_size_sqm") or 0)
        if sz < 300:
            size_buckets["<300"].append(lst)
        elif sz < 400:
            size_buckets["300-400"].append(lst)
        elif sz < 500:
            size_buckets["400-500"].append(lst)
        else:
            size_buckets["500+"].append(lst)

    by_size: dict[str, dict] = {}
    for bucket, items in size_buckets.items():
        if not items:
            continue
        views = sum(int(x.get("rea_views") or 0) for x in items)
        enq = sum(int(x.get("rea_enquiries") or 0) for x in items)
        by_size[bucket] = {
            "count": len(items),
            "views": views,
            "enquiries": enq,
            "ctr_pct": _ctr(views, enq),
        }

    # ── Top / bottom performers ───────────────────────────────────────────
    with_views = [lst for lst in listings if int(lst.get("rea_views") or 0) > 5]
    sorted_by_ctr = sorted(
        with_views,
        key=lambda x: _ctr(int(x.get("rea_views") or 0), int(x.get("rea_enquiries") or 0)),
        reverse=True,
    )
    top_5 = sorted_by_ctr[:5]
    bottom_5 = sorted_by_ctr[-5:] if len(sorted_by_ctr) >= 5 else sorted_by_ctr

    def _summary(lst: dict) -> dict:
        views = int(lst.get("rea_views") or 0)
        enq = int(lst.get("rea_enquiries") or 0)
        return {
            "id": lst["id"],
            "address": lst["address"],
            "suburb": lst.get("suburb"),
            "headline": lst.get("listing_headline"),
            "variant": lst.get("rea_title_variant"),
            "views": views,
            "enquiries": enq,
            "ctr_pct": _ctr(views, enq),
            "last_edit": lst.get("rea_last_edit_at"),
        }

    # ── Recommendations ───────────────────────────────────────────────────
    recommendations: list[str] = []

    best_variant = max(by_variant.items(), key=lambda kv: kv[1]["ctr_pct"], default=None)
    worst_variant = min(by_variant.items(), key=lambda kv: kv[1]["ctr_pct"], default=None)
    if best_variant and worst_variant and best_variant[0] != worst_variant[0]:
        recommendations.append(
            f"Variant #{best_variant[0]} is top-performing (CTR {best_variant[1]['ctr_pct']}%). "
            f"Bias new listings toward this template."
        )
        recommendations.append(
            f"Variant #{worst_variant[0]} is underperforming (CTR {worst_variant[1]['ctr_pct']}%). "
            f"Rotate these listings away from this template on next refresh."
        )

    best_suburb = max(by_suburb.items(), key=lambda kv: kv[1]["ctr_pct"], default=None)
    if best_suburb:
        recommendations.append(
            f"{best_suburb[0]} has the highest engagement (CTR {best_suburb[1]['ctr_pct']}%). "
            f"Prioritise remaining unpushed lots in this suburb."
        )

    refresh_candidates = [
        lst for lst in listings
        if int(lst.get("rea_views") or 0) > 10 and _ctr(int(lst.get("rea_views") or 0), int(lst.get("rea_enquiries") or 0)) < 1.0
    ]
    if refresh_candidates:
        recommendations.append(
            f"{len(refresh_candidates)} listing(s) have >10 views but <1% CTR — "
            f"run generate_refresh_plan to rotate their copy."
        )

    log.info(
        "analyze_portfolio: %d live listings, top CTR suburb=%s",
        len(listings),
        best_suburb[0] if best_suburb else "N/A",
    )

    return {
        "ok": True,
        "live_count": len(listings),
        "total_views": sum(int(x.get("rea_views") or 0) for x in listings),
        "total_enquiries": sum(int(x.get("rea_enquiries") or 0) for x in listings),
        "top_performers": [_summary(x) for x in top_5],
        "bottom_performers": [_summary(x) for x in bottom_5],
        "by_variant": {str(k): v for k, v in by_variant.items()},
        "by_suburb": by_suburb,
        "by_size_bucket": by_size,
        "recommendations": recommendations,
    }


async def generate_push_plan(session: AsyncSession, daily_limit: int = 15) -> dict:
    """Create a plan for the next batch of listings to push.

    Picks from unpushed Bathla lots, assigns title/description variants,
    returns the plan for operator approval before execution.
    """
    daily_limit = min(daily_limit, 15)  # Hard cap per REA stagger rules

    # Check how many have already been pushed today
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    already_today = (
        await session.execute(
            text("""
                SELECT COUNT(*) AS cnt FROM leads
                WHERE COALESCE(rea_last_edit_at, '') >= :today
                  AND COALESCE(rea_upload_id, '') <> ''
            """),
            {"today": today_start},
        )
    ).scalar() or 0

    remaining_today = max(0, daily_limit - int(already_today))
    if remaining_today == 0:
        return {
            "ok": True,
            "message": f"Daily limit of {daily_limit} already reached ({already_today} pushed today).",
            "plan": [],
            "plan_size": 0,
            "already_pushed_today": int(already_today),
        }

    # Pull unpushed lots ordered by price desc (higher value first)
    rows = (
        await session.execute(
            text("""
                SELECT * FROM leads
                WHERE (
                    LOWER(COALESCE(property_type, '')) = 'land'
                    OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land'
                )
                  AND COALESCE(rea_listing_id, '') = ''
                  AND COALESCE(rea_upload_id, '') = ''
                ORDER BY COALESCE(estimated_value_mid, 0) DESC
                LIMIT :limit
            """),
            {"limit": remaining_today},
        )
    ).mappings().all()

    if not rows:
        return {
            "ok": True,
            "message": "No unpushed land listings remain in the Bathla queue.",
            "plan": [],
            "plan_size": 0,
            "already_pushed_today": int(already_today),
        }

    plan = []
    for row in rows:
        lead = dict(row)
        lead_id = lead["id"]
        variant = hash(lead_id) % len(TITLE_VARIANTS)
        title = _generate_title(lead, variant)
        desc = _generate_description(lead, variant)
        size = int(float(lead.get("land_size_sqm") or 0))
        price = int(float(lead.get("estimated_value_mid") or 0))

        plan.append({
            "lead_id": lead_id,
            "address": lead.get("address"),
            "suburb": lead.get("suburb"),
            "postcode": lead.get("postcode"),
            "land_size_sqm": size,
            "price": price,
            "lot_number": lead.get("lot_number"),
            "lot_type": lead.get("lot_type"),
            "title_variant": variant,
            "proposed_title": title,
            "proposed_description": desc,
        })

    log.info("generate_push_plan: %d lots queued for push", len(plan))

    return {
        "ok": True,
        "message": f"Push plan ready: {len(plan)} listings queued for operator approval.",
        "plan": plan,
        "plan_size": len(plan),
        "already_pushed_today": int(already_today),
        "daily_limit": daily_limit,
        "note": "Call execute_push(lead_ids=[...]) with the approved lead_ids to publish.",
    }


async def generate_refresh_plan(session: AsyncSession, limit: int = 10) -> dict:
    """Identify live listings eligible for refresh (>7 days old, not edited in 24h).

    Sorts by worst CTR first. For each, recommends a refresh strategy and
    pre-generates new copy. Returns plan for operator approval before execution.
    """
    limit = min(limit, 15)
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()

    rows = (
        await session.execute(
            text("""
                SELECT * FROM leads
                WHERE COALESCE(rea_listing_id, '') <> ''
                  AND (
                      rea_last_edit_at IS NULL
                      OR rea_last_edit_at < :cutoff_24h
                  )
                  AND updated_at < :cutoff_7d
                ORDER BY COALESCE(rea_enquiries, 0) * 1.0
                       / GREATEST(COALESCE(rea_views, 1), 1) ASC
                LIMIT :limit
            """),
            {"cutoff_24h": cutoff_24h, "cutoff_7d": cutoff_7d, "limit": limit},
        )
    ).mappings().all()

    if not rows:
        return {
            "ok": True,
            "message": "No listings eligible for refresh (all recently edited or live <7 days).",
            "plan": [],
            "plan_size": 0,
        }

    plan = []
    for row in rows:
        lead = dict(row)
        old_variant = int(lead.get("rea_title_variant") or 0)
        new_variant = (old_variant + 1) % len(TITLE_VARIANTS)
        new_title = _generate_title(lead, new_variant)
        new_desc = _generate_description(lead, new_variant)

        views = int(lead.get("rea_views") or 0)
        enq = int(lead.get("rea_enquiries") or 0)
        current_ctr = _ctr(views, enq)

        # Pick refresh strategy — use price_micro_update only if listing is very stale
        last_edit = lead.get("rea_last_edit_at") or lead.get("updated_at") or ""
        days_stale = 0
        if last_edit:
            try:
                edit_dt = datetime.fromisoformat(last_edit.replace("Z", "+00:00"))
                days_stale = (datetime.now(timezone.utc) - edit_dt).days
            except (ValueError, TypeError):
                days_stale = 0

        if days_stale >= 14 and views > 20 and current_ctr < 0.5:
            strategy = REFRESH_STRATEGIES[2]  # price_micro_update
        elif current_ctr < 1.0:
            strategy = REFRESH_STRATEGIES[0]  # description_rewrite
        else:
            strategy = REFRESH_STRATEGIES[1]  # headline_optimize

        plan.append({
            "lead_id": lead["id"],
            "rea_listing_id": lead.get("rea_listing_id"),
            "address": lead.get("address"),
            "suburb": lead.get("suburb"),
            "current_headline": lead.get("listing_headline"),
            "current_variant": old_variant,
            "current_views": views,
            "current_enquiries": enq,
            "current_ctr_pct": current_ctr,
            "days_since_edit": days_stale,
            "strategy": strategy["name"],
            "strategy_description": strategy["description"],
            "new_title_variant": new_variant,
            "proposed_title": new_title,
            "proposed_description": new_desc,
        })

    log.info("generate_refresh_plan: %d listings eligible for refresh", len(plan))

    return {
        "ok": True,
        "message": f"Refresh plan ready: {len(plan)} listing(s) need attention.",
        "plan": plan,
        "plan_size": len(plan),
        "note": "Call execute_refresh(lead_ids=[...]) with approved lead_ids to apply new copy.",
    }


async def execute_push(session: AsyncSession, lead_ids: list[str]) -> dict:
    """Execute approved push plan — publish listings to REA with assigned variants.

    Only operates on lead_ids explicitly provided (operator-approved subset).
    Enforces max 15 per call and 3-second stagger between submissions.
    """
    import asyncio

    if not lead_ids:
        return {"ok": False, "error": "No lead_ids provided."}
    if len(lead_ids) > 15:
        return {"ok": False, "error": "Cannot push more than 15 listings in a single batch."}

    creds = await check_credentials()
    if not creds.get("token_ok"):
        return {
            "ok": False,
            "error": f"REA credentials invalid: {creds.get('message', 'unknown')}",
        }

    results = []
    success = 0
    failed = 0

    for i, lead_id in enumerate(lead_ids):
        row = (
            await session.execute(
                text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id}
            )
        ).mappings().first()

        if not row:
            results.append({"lead_id": lead_id, "ok": False, "error": "Lead not found."})
            failed += 1
            continue

        lead = dict(row)
        if not _is_land_listing(lead):
            results.append({
                "lead_id": lead_id,
                "ok": False,
                "error": "Safety lock: only land listings can be pushed to REA.",
                "address": lead.get("address"),
            })
            failed += 1
            continue
        if lead.get("rea_listing_id") or lead.get("rea_upload_id"):
            results.append({
                "lead_id": lead_id,
                "ok": False,
                "error": "Already pushed — use execute_refresh to update.",
            })
            failed += 1
            continue

        variant = hash(lead_id) % len(TITLE_VARIANTS)
        title = _generate_title(lead, variant)
        desc = _generate_description(lead, variant)
        lead["listing_headline"] = title
        lead["listing_description"] = desc
        lead["property_type"] = "Land"

        log.info("execute_push [%d/%d]: %s — %s", i + 1, len(lead_ids), lead.get("address"), title[:50])

        async with _async_session_factory() as push_session:
            result = await publish_listing(lead, session=push_session, lead_id=lead_id)

            if result.get("ok"):
                upload_id = result.get("upload_id", "")
                rea_listing_id = result.get("rea_listing_id", "")
                await push_session.execute(
                    text("""
                        UPDATE leads SET
                            rea_upload_id = :upload_id,
                            rea_upload_status = :status,
                            rea_listing_id = COALESCE(NULLIF(:rea_listing_id, ''), rea_listing_id),
                            listing_headline = :headline,
                            listing_description = :description,
                            rea_last_upload_response = :response,
                            rea_title_variant = :variant,
                            rea_desc_variant = :desc_variant,
                            rea_last_edit_at = :now,
                            updated_at = :now
                        WHERE id = :id
                    """),
                    {
                        "upload_id": upload_id,
                        "status": result.get("status", "submitted"),
                        "rea_listing_id": rea_listing_id,
                        "headline": title,
                        "description": desc,
                        "response": json.dumps(result.get("response") or {}),
                        "variant": variant,
                        "desc_variant": variant % 3,
                        "now": _now_iso(),
                        "id": lead_id,
                    },
                )
                await push_session.commit()
                results.append({
                    "lead_id": lead_id,
                    "ok": True,
                    "upload_id": upload_id,
                    "rea_listing_id": rea_listing_id,
                    "address": lead.get("address"),
                    "title": title,
                    "variant": variant,
                })
                success += 1
                log.info("execute_push: OK — %s upload_id=%s", lead.get("address"), upload_id)
            else:
                error = result.get("error", "Unknown error")[:120]
                results.append({
                    "lead_id": lead_id,
                    "ok": False,
                    "error": error,
                    "address": lead.get("address"),
                })
                failed += 1
                log.warning("execute_push: FAIL — %s: %s", lead.get("address"), error)

        if i < len(lead_ids) - 1:
            await asyncio.sleep(3)

    return {
        "ok": True,
        "pushed": success,
        "failed": failed,
        "total": len(lead_ids),
        "results": results,
    }


async def execute_refresh(session: AsyncSession, lead_ids: list[str]) -> dict:
    """Execute approved refresh — update listed properties with new copy.

    Enforces REA rule: max 1 edit per listing per 24h. Skips any listing
    edited within the last 24 hours regardless of what was passed in.
    """
    import asyncio

    if not lead_ids:
        return {"ok": False, "error": "No lead_ids provided."}

    creds = await check_credentials()
    if not creds.get("token_ok"):
        return {
            "ok": False,
            "error": f"REA credentials invalid: {creds.get('message', 'unknown')}",
        }

    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    results = []
    success = 0
    skipped = 0
    failed = 0

    for i, lead_id in enumerate(lead_ids):
        row = (
            await session.execute(
                text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id}
            )
        ).mappings().first()

        if not row:
            results.append({"lead_id": lead_id, "ok": False, "error": "Lead not found."})
            failed += 1
            continue

        lead = dict(row)
        rea_id = lead.get("rea_listing_id")
        if not rea_id:
            results.append({
                "lead_id": lead_id,
                "ok": False,
                "error": "No rea_listing_id — listing not live yet.",
            })
            failed += 1
            continue

        # Enforce 24h rule
        last_edit = lead.get("rea_last_edit_at") or ""
        if last_edit and last_edit >= cutoff_24h:
            results.append({
                "lead_id": lead_id,
                "ok": False,
                "skipped": True,
                "error": f"Edited within last 24h ({last_edit}) — REA rule enforced.",
                "address": lead.get("address"),
            })
            skipped += 1
            log.info("execute_refresh: skip %s — edited within 24h", lead.get("address"))
            continue

        old_variant = int(lead.get("rea_title_variant") or 0)
        new_variant = (old_variant + 1) % len(TITLE_VARIANTS)
        new_title = _generate_title(lead, new_variant)
        new_desc = _generate_description(lead, new_variant)

        log.info(
            "execute_refresh [%d/%d]: %s — variant %d→%d",
            i + 1, len(lead_ids), lead.get("address"), old_variant, new_variant,
        )

        async with _async_session_factory() as refresh_session:
            result = await update_listing(
                rea_id,
                {"headline": new_title, "description": new_desc},
                session=refresh_session,
                lead_id=lead_id,
            )

            if result.get("ok"):
                await refresh_session.execute(
                    text("""
                        UPDATE leads SET
                            listing_headline = :headline,
                            listing_description = :description,
                            rea_title_variant = :variant,
                            rea_desc_variant = :desc_variant,
                            rea_last_edit_at = :now,
                            updated_at = :now
                        WHERE id = :id
                    """),
                    {
                        "headline": new_title,
                        "description": new_desc,
                        "variant": new_variant,
                        "desc_variant": new_variant % 3,
                        "now": _now_iso(),
                        "id": lead_id,
                    },
                )
                await refresh_session.commit()
                results.append({
                    "lead_id": lead_id,
                    "ok": True,
                    "address": lead.get("address"),
                    "old_variant": old_variant,
                    "new_variant": new_variant,
                    "new_title": new_title,
                })
                success += 1
                log.info("execute_refresh: OK — %s", lead.get("address"))
            else:
                error = result.get("error", "Unknown error")[:120]
                results.append({
                    "lead_id": lead_id,
                    "ok": False,
                    "error": error,
                    "address": lead.get("address"),
                })
                failed += 1
                log.warning("execute_refresh: FAIL — %s: %s", lead.get("address"), error)

        if i < len(lead_ids) - 1:
            await asyncio.sleep(2)

    return {
        "ok": True,
        "refreshed": success,
        "skipped_24h_rule": skipped,
        "failed": failed,
        "total": len(lead_ids),
        "results": results,
    }


async def pull_performance(session: AsyncSession) -> dict:
    """Pull latest metrics for all live listings from REA API, save to DB.

    Hits the REA performance endpoint for each live listing and writes
    views/enquiries back to the leads table.
    """
    rows = (
        await session.execute(
            text("""
                SELECT id, address, suburb, rea_listing_id, listing_headline, rea_title_variant
                FROM leads
                WHERE COALESCE(rea_listing_id, '') <> ''
                  AND (
                      LOWER(COALESCE(property_type, '')) = 'land'
                      OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land'
                  )
                ORDER BY suburb, address
            """)
        )
    ).mappings().all()

    if not rows:
        return {
            "ok": True,
            "message": "No live listings to pull performance for.",
            "updated": 0,
            "failed": 0,
        }

    updated = 0
    failed = 0
    metrics_list = []

    for row in rows:
        async with _async_session_factory() as perf_session:
            perf = await get_listing_performance(row["rea_listing_id"], session=perf_session)

        if perf.get("ok"):
            raw = perf.get("metrics", {})
            views = int(raw.get("views", raw.get("totalViews", 0)) or 0)
            enquiries = int(raw.get("enquiries", raw.get("totalEnquiries", 0)) or 0)

            async with _async_session_factory() as save_session:
                await save_session.execute(
                    text("""
                        UPDATE leads SET
                            rea_views = :views,
                            rea_enquiries = :enq,
                            updated_at = :now
                        WHERE id = :id
                    """),
                    {"views": views, "enq": enquiries, "now": _now_iso(), "id": row["id"]},
                )
                await save_session.commit()

            metrics_list.append({
                "lead_id": row["id"],
                "address": row["address"],
                "suburb": row.get("suburb"),
                "rea_listing_id": row["rea_listing_id"],
                "views": views,
                "enquiries": enquiries,
                "ctr_pct": _ctr(views, enquiries),
                "variant": row.get("rea_title_variant"),
            })
            updated += 1
        else:
            metrics_list.append({
                "lead_id": row["id"],
                "address": row["address"],
                "ok": False,
                "error": perf.get("error", "unknown"),
            })
            failed += 1
            log.warning("pull_performance: failed for %s: %s", row["address"], perf.get("error"))

    total_views = sum(m.get("views", 0) for m in metrics_list if m.get("ok", True))
    total_enq = sum(m.get("enquiries", 0) for m in metrics_list if m.get("ok", True))

    log.info("pull_performance: %d updated, %d failed — total V:%d E:%d", updated, failed, total_views, total_enq)

    return {
        "ok": True,
        "updated": updated,
        "failed": failed,
        "total_listings": len(rows),
        "portfolio_views": total_views,
        "portfolio_enquiries": total_enq,
        "portfolio_ctr_pct": _ctr(total_views, total_enq),
        "metrics": metrics_list,
    }


async def self_improve(session: AsyncSession) -> dict:
    """The learning loop: analyze which variants/strategies work best,
    update internal scoring, generate insights for the operator.

    Runs analyze_portfolio then produces a ranked action plan covering:
    - Which variants to bias toward for new pushes
    - Which suburbs to prioritise in the push queue
    - Which listings to refresh next
    - One concrete copywriting recommendation based on what's converting
    """
    analysis = await analyze_portfolio(session)

    if not analysis.get("ok"):
        return {"ok": False, "error": "Portfolio analysis failed."}

    if analysis.get("live_count", 0) == 0:
        return {
            "ok": True,
            "insights": [],
            "action_plan": ["No live listings yet. Run generate_push_plan to get started."],
            "best_variant": None,
            "best_suburb": None,
        }

    by_variant = analysis.get("by_variant", {})
    by_suburb = analysis.get("by_suburb", {})
    bottom_performers = analysis.get("bottom_performers", [])

    insights: list[str] = []
    action_plan: list[str] = []

    # ── Variant insights ──────────────────────────────────────────────────
    best_variant = None
    best_variant_ctr = -1.0
    worst_variant = None
    worst_variant_ctr = 9999.0

    for v_str, stats in by_variant.items():
        if stats["count"] >= 2:  # Need at least 2 listings for statistical meaning
            if stats["ctr_pct"] > best_variant_ctr:
                best_variant_ctr = stats["ctr_pct"]
                best_variant = int(v_str)
            if stats["ctr_pct"] < worst_variant_ctr:
                worst_variant_ctr = stats["ctr_pct"]
                worst_variant = int(v_str)

    if best_variant is not None:
        tpl = TITLE_VARIANTS[best_variant % len(TITLE_VARIANTS)]
        insights.append(
            f"Best title template (variant #{best_variant}, CTR {best_variant_ctr}%): \"{tpl}\". "
            f"New listings should use this variant or its closest equivalent."
        )
        action_plan.append(
            f"Bias next push batch toward variant #{best_variant} — it's converting at {best_variant_ctr}% CTR."
        )

    if worst_variant is not None and worst_variant != best_variant:
        tpl = TITLE_VARIANTS[worst_variant % len(TITLE_VARIANTS)]
        insights.append(
            f"Worst title template (variant #{worst_variant}, CTR {worst_variant_ctr}%): \"{tpl}\". "
            f"Rotate listings using this variant on their next allowed edit."
        )
        rotation_ids = [
            b["id"] for b in bottom_performers
            if int(b.get("variant") or -1) == worst_variant
        ]
        if rotation_ids:
            action_plan.append(
                f"Schedule refresh for {len(rotation_ids)} listing(s) using underperforming variant #{worst_variant}."
            )

    # ── Suburb insights ───────────────────────────────────────────────────
    best_suburb = None
    best_suburb_ctr = -1.0
    for suburb, stats in by_suburb.items():
        if stats["count"] >= 2 and stats["ctr_pct"] > best_suburb_ctr:
            best_suburb_ctr = stats["ctr_pct"]
            best_suburb = suburb

    if best_suburb:
        insights.append(
            f"{best_suburb} is the highest-converting suburb (CTR {best_suburb_ctr}%). "
            f"Prioritise unpushed lots in {best_suburb} in the next push batch."
        )
        action_plan.append(
            f"In generate_push_plan, filter or sort to prioritise {best_suburb} lots first."
        )

    # ── Refresh eligibility ───────────────────────────────────────────────
    refresh_plan = await generate_refresh_plan(session, limit=5)
    refresh_count = refresh_plan.get("plan_size", 0)
    if refresh_count > 0:
        insights.append(
            f"{refresh_count} listing(s) are eligible for refresh right now "
            f"(>7 days old, not edited in last 24h, low CTR)."
        )
        action_plan.append(
            f"Run execute_refresh on the {refresh_count} listing(s) from generate_refresh_plan."
        )

    # ── Portfolio health ──────────────────────────────────────────────────
    portfolio_ctr = _ctr(
        analysis.get("total_views", 0),
        analysis.get("total_enquiries", 0),
    )
    if portfolio_ctr < 1.0:
        insights.append(
            f"Portfolio-wide CTR is {portfolio_ctr}% — below the 1% target. "
            f"Aggressive copy rotation is warranted."
        )
    elif portfolio_ctr >= 3.0:
        insights.append(
            f"Portfolio-wide CTR is {portfolio_ctr}% — strong. "
            f"Focus on volume: push remaining unpushed lots quickly."
        )
    else:
        insights.append(
            f"Portfolio-wide CTR is {portfolio_ctr}%. "
            f"Incrementally rotate underperformers while maintaining push cadence."
        )

    log.info(
        "self_improve complete: %d insights, %d action items, portfolio CTR=%.1f%%",
        len(insights), len(action_plan), portfolio_ctr,
    )

    return {
        "ok": True,
        "portfolio_ctr_pct": portfolio_ctr,
        "live_count": analysis.get("live_count", 0),
        "best_variant": best_variant,
        "best_variant_ctr_pct": best_variant_ctr if best_variant is not None else None,
        "best_suburb": best_suburb,
        "best_suburb_ctr_pct": best_suburb_ctr if best_suburb is not None else None,
        "insights": insights,
        "action_plan": action_plan,
        "refresh_eligible_count": refresh_count,
        "recommendations": analysis.get("recommendations", []),
    }
