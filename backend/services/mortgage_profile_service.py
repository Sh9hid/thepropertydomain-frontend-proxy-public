from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from models.sql_models import CallLog, Lead, LeadMortgageProfile, MortgageOpportunityFeedback


PRIORITY_LENDER_ALIASES = {
    "cba": "CommBank",
    "commbank": "CommBank",
    "commonwealth bank": "CommBank",
    "nab": "NATIONAL AUSTRALIA BANK",
    "national australia bank": "NATIONAL AUSTRALIA BANK",
    "anz": "ANZ",
    "westpac": "Westpac",
    "st george": "St.George Bank",
    "st.george": "St.George Bank",
}

SUPPRESSION_TYPES = {
    "not_mortgage_lead",
    "wrong_occupancy",
    "already_refinanced",
    "too_small_to_matter",
    "wrong_loan_assumption",
    "smsf_not_relevant",
}


def _now_iso() -> str:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0).isoformat()


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _safe_int(value: Any) -> Optional[int]:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def extract_mortgage_facts_from_text(text: str) -> Dict[str, Any]:
    raw = str(text or "")
    lowered = raw.lower()
    extracted_facts: List[Dict[str, Any]] = []

    lender = None
    for alias, canonical in PRIORITY_LENDER_ALIASES.items():
        if alias in lowered:
            lender = canonical
            extracted_facts.append({"field": "current_lender", "value": canonical, "reason": f"Mentioned in transcript as '{alias}'"})
            break

    rate_match = re.search(r"(\d(?:\.\d{1,2})?)\s*%", raw)
    current_rate = _safe_float(rate_match.group(1)) if rate_match else None
    if current_rate is not None:
        extracted_facts.append({"field": "current_rate", "value": current_rate, "reason": "Percentage rate mentioned in transcript"})

    fixed_or_variable = None
    if "fixed" in lowered:
        fixed_or_variable = "fixed"
    elif "variable" in lowered:
        fixed_or_variable = "variable"
    if fixed_or_variable:
        extracted_facts.append({"field": "fixed_or_variable", "value": fixed_or_variable, "reason": "Rate type mentioned in transcript"})

    balance_match = re.search(r"\$?\s*([2-9]\d{2},?\d{3}|[1-9]\d{5,6})", raw)
    loan_balance_estimate = None
    if balance_match:
        balance_value = int(balance_match.group(1).replace(",", ""))
        if 100_000 <= balance_value <= 5_000_000:
            loan_balance_estimate = balance_value
            extracted_facts.append({"field": "loan_balance_estimate", "value": balance_value, "reason": "Loan-sized dollar amount mentioned in transcript"})

    offset_account = None
    if "offset" in lowered:
        offset_account = True
        extracted_facts.append({"field": "offset_account", "value": True, "reason": "Offset account mentioned in transcript"})

    owner_occupancy_confirmed = None
    if "owner occup" in lowered or "live in the property" in lowered:
        owner_occupancy_confirmed = "owner_occupier"
    elif "investor" in lowered or "rented out" in lowered:
        owner_occupancy_confirmed = "investor"
    if owner_occupancy_confirmed:
        extracted_facts.append({"field": "owner_occupancy_confirmed", "value": owner_occupancy_confirmed, "reason": "Occupancy wording found in transcript"})

    refinance_interest = None
    if "refinance" in lowered or "switch lender" in lowered or "better rate" in lowered:
        refinance_interest = "discussed"
        extracted_facts.append({"field": "refinance_interest", "value": "discussed", "reason": "Refinance intent wording found in transcript"})

    fixed_expiry = None
    expiry_match = re.search(r"(fixed.*?(?:ends|ending|expiry|expires).*?(?:\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b.*?\d{4}|\d{4}))", lowered)
    if expiry_match:
        fixed_expiry = expiry_match.group(1)
        extracted_facts.append({"field": "fixed_expiry", "value": fixed_expiry, "reason": "Fixed expiry wording found in transcript"})

    return {
        "current_lender": lender,
        "current_rate": current_rate,
        "loan_balance_estimate": loan_balance_estimate,
        "fixed_or_variable": fixed_or_variable,
        "offset_account": offset_account,
        "owner_occupancy_confirmed": owner_occupancy_confirmed,
        "refinance_interest": refinance_interest,
        "fixed_expiry": fixed_expiry,
        "extracted_facts_json": extracted_facts,
    }


async def get_or_create_mortgage_profile(session: AsyncSession, lead_id: str) -> LeadMortgageProfile:
    profile = await session.get(LeadMortgageProfile, lead_id)
    if profile:
        return profile
    profile = LeadMortgageProfile(lead_id=lead_id, created_at=_now_iso(), updated_at=_now_iso())
    session.add(profile)
    await session.commit()
    await session.refresh(profile)
    return profile


async def update_mortgage_profile(session: AsyncSession, lead_id: str, payload: Dict[str, Any], updated_by: str = "operator") -> LeadMortgageProfile:
    profile = await get_or_create_mortgage_profile(session, lead_id)
    for key, value in payload.items():
        if hasattr(profile, key) and value is not None:
            setattr(profile, key, value)
    profile.updated_by = updated_by
    profile.updated_at = _now_iso()
    await session.commit()
    await session.refresh(profile)
    return profile


async def extract_mortgage_profile_from_calls(session: AsyncSession, lead_id: str) -> LeadMortgageProfile:
    rows = (
        await session.execute(
            select(CallLog)
            .where(CallLog.lead_id == lead_id)
            .order_by(CallLog.timestamp.desc(), CallLog.logged_at.desc())
            .limit(12)
        )
    ).scalars().all()
    merged: Dict[str, Any] = {"extracted_facts_json": []}
    for row in rows:
        text_blob = "\n".join([str(row.transcript or ""), str(row.summary or ""), str(row.note or "")]).strip()
        if not text_blob:
            continue
        extracted = extract_mortgage_facts_from_text(text_blob)
        for key, value in extracted.items():
            if key == "extracted_facts_json":
                merged.setdefault("extracted_facts_json", []).extend(value or [])
            elif value not in (None, "", []):
                merged.setdefault(key, value)
        if any(merged.get(field) for field in ("current_lender", "current_rate", "loan_balance_estimate", "fixed_or_variable")):
            break
    merged["provenance_json"] = {"source": "call_log", "scanned_calls": len(rows)}
    return await update_mortgage_profile(session, lead_id, merged, updated_by="transcript_extraction")


def profile_balance_band(profile: Optional[LeadMortgageProfile]) -> Optional[str]:
    if not profile or not profile.loan_balance_estimate:
        return profile.loan_balance_band if profile else None
    amount = int(profile.loan_balance_estimate)
    if amount < 300_000:
        return "<300k"
    if amount < 500_000:
        return "300-500k"
    if amount < 800_000:
        return "500-800k"
    return "800k+"


async def list_active_feedback(session: AsyncSession, lead_id: str) -> List[MortgageOpportunityFeedback]:
    rows = await session.execute(
        select(MortgageOpportunityFeedback)
        .where(MortgageOpportunityFeedback.lead_id == lead_id)
        .where(MortgageOpportunityFeedback.active == True)  # noqa: E712
        .order_by(MortgageOpportunityFeedback.created_at.desc())
    )
    return list(rows.scalars().all())


async def add_feedback(
    session: AsyncSession,
    lead_id: str,
    feedback_type: str,
    *,
    opportunity_type: Optional[str] = None,
    note: Optional[str] = None,
    created_by: str = "operator",
) -> MortgageOpportunityFeedback:
    row = MortgageOpportunityFeedback(
        id=str(uuid.uuid4()),
        lead_id=lead_id,
        opportunity_type=opportunity_type,
        feedback_type=feedback_type,
        note=note,
        created_by=created_by,
        active=True,
        created_at=_now_iso(),
        updated_at=_now_iso(),
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


def should_suppress_opportunity(feedback_rows: List[MortgageOpportunityFeedback], opportunity_type: str) -> Optional[str]:
    for row in feedback_rows:
        if row.feedback_type in SUPPRESSION_TYPES and (row.opportunity_type in {None, "", opportunity_type}):
            return row.feedback_type
    return None


def suburb_cross_check_summary(lead: Lead) -> Dict[str, Any]:
    nearby_sales = getattr(lead, "nearby_sales", None) or []
    sale_prices = []
    for item in nearby_sales:
        if not isinstance(item, dict):
            continue
        price = item.get("sale_price") or item.get("sold_price") or item.get("price")
        parsed = _safe_int(price)
        if parsed:
            sale_prices.append(parsed)
    value_low = _safe_int(getattr(lead, "estimated_value_low", None))
    value_high = _safe_int(getattr(lead, "estimated_value_high", None))
    value_mid = int((value_low + value_high) / 2) if value_low and value_high else None
    if not sale_prices or not value_mid:
        return {"status": "insufficient", "reason": "No nearby-sale median available for cross-check"}
    median = sorted(sale_prices)[len(sale_prices) // 2]
    variance = abs(value_mid - median) / max(median, 1)
    if variance > 0.35:
        return {
            "status": "mismatch",
            "reason": f"The inferred valuation band differs materially from nearby sales. The current midpoint is about ${value_mid:,.0f} while nearby sales center closer to ${median:,.0f}.",
            "median_sale_price": median,
            "value_midpoint": value_mid,
        }
    return {
        "status": "confirmed",
        "reason": f"The inferred valuation band is broadly in line with nearby sales. The current midpoint is about ${value_mid:,.0f} and nearby sales center around ${median:,.0f}.",
        "median_sale_price": median,
        "value_midpoint": value_mid,
    }

