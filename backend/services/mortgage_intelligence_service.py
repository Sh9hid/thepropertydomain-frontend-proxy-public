from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import delete, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from core.config import SYDNEY_TZ
from models.sql_models import BankDataHolder, Lead, LeadMortgageProfile, MortgageOpportunity
from services.lender_product_service import best_market_rate
from services.cdr_lenders import get_cdr_bank_registry
from services.mortgage_profile_service import (
    list_active_feedback,
    profile_balance_band,
    should_suppress_opportunity,
    suburb_cross_check_summary,
)


ACTIVE_OPPORTUNITY_WINDOW_DAYS = 14
ASSUMED_REFI_MARKET_RATE = 5.94
ASSUMED_BAD_OWNER_OCC_RATE = 6.74
ASSUMED_BAD_INVESTOR_RATE = 7.02
ASSUMED_FIXED_REVIEW_RATE = 5.79


def _now_iso() -> str:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0).isoformat()


def _safe_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_signal_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _sentence(value: str) -> str:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return ""
    if text[-1] not in ".!?":
        text = f"{text}."
    return text[0].upper() + text[1:]


def _monthly_payment(principal: int, annual_rate: float, years: int = 25) -> float:
    if principal <= 0:
        return 0.0
    monthly_rate = annual_rate / 100.0 / 12.0
    periods = years * 12
    if monthly_rate <= 0:
        return principal / max(periods, 1)
    factor = math.pow(1 + monthly_rate, periods)
    return principal * (monthly_rate * factor) / (factor - 1)


def _value_midpoint(lead: Lead) -> int:
    low = _safe_int(getattr(lead, "estimated_value_low", None))
    high = _safe_int(getattr(lead, "estimated_value_high", None))
    est_value = _safe_int(getattr(lead, "est_value", None))
    if low and high:
        return int((low + high) / 2)
    if est_value:
        return est_value
    return max(low, high)


def _estimated_loan_amount(lead: Lead, value_mid: int) -> int:
    sale_price = _safe_int(getattr(lead, "sale_price", None))
    ownership_years = _safe_float(getattr(lead, "ownership_duration_years", None))
    if sale_price > 0:
        debt_factor = 0.82 if ownership_years < 3 else 0.72 if ownership_years < 7 else 0.56
        return int(max(220_000, sale_price * debt_factor))
    if value_mid > 0:
        return int(max(220_000, value_mid * 0.58))
    return 0


def _loan_amount_from_profile(profile: Optional[LeadMortgageProfile], fallback_amount: int) -> int:
    if profile and profile.loan_balance_estimate:
        return int(profile.loan_balance_estimate)
    return fallback_amount


def _owner_profile(lead: Lead) -> str:
    status = str(getattr(lead, "owner_occupancy_status", "") or "").lower()
    if "owner" in status:
        return "owner_occupier"
    if "invest" in status:
        return "investor"
    if getattr(lead, "likely_owner_occupier", False):
        return "owner_occupier"
    if getattr(lead, "likely_landlord", False) or getattr(lead, "absentee_owner", False):
        return "investor"
    return "unknown"


def _evidence_rows(lead: Lead, value_mid: int, loan_amount: int, profile: str, signal_count: int) -> List[Dict[str, Any]]:
    evidence: List[Dict[str, Any]] = []
    if profile == "owner_occupier":
        evidence.append({"label": "Owner-occupier likely", "value": _sentence("Mailing and occupancy clues point to an owner-occupied property")})
    elif profile == "investor":
        evidence.append({"label": "Investor/landlord likely", "value": _sentence("Absentee ownership or landlord clues are present")})
    if value_mid:
        evidence.append({"label": "Estimated property value", "value": _sentence(f"The current inferred value band centers around ${value_mid:,.0f}")})
    if loan_amount:
        evidence.append({"label": "Estimated loan band", "value": _sentence(f"The working loan estimate is about ${loan_amount:,.0f}")})
    ownership_years = _safe_float(getattr(lead, "ownership_duration_years", None))
    if ownership_years:
        evidence.append({"label": "Ownership duration", "value": _sentence(f"The recorded ownership period is about {ownership_years:.1f} years")})
    if signal_count:
        evidence.append({"label": "Refinance signals", "value": _sentence(f"There are already {signal_count} deterministic mortgage clues on this lead")})
    return evidence


def _lead_story_sentence(lead: Lead, profile: str, value_mid: int, ownership_years: float) -> str:
    address = str(getattr(lead, "address", "") or "This property")
    profile_text = (
        "appears to be owner-occupied"
        if profile == "owner_occupier"
        else "looks more like an investor-owned property"
        if profile == "investor"
        else "has mixed occupancy signals"
    )
    value_text = f"and the current value band is around ${value_mid:,.0f}" if value_mid else "and the current value band is still being inferred"
    years_text = f"after about {ownership_years:.1f} years of ownership" if ownership_years else "with no confirmed ownership duration"
    return _sentence(f"{address} {profile_text} {years_text}, {value_text}")


@dataclass
class _OpportunityDraft:
    opportunity_type: str
    priority_score: float
    headline: str
    reason_to_call: str
    why_now: str
    next_best_action: str
    best_call_window: str
    current_rate_estimate: Optional[float]
    market_rate_estimate: Optional[float]
    estimated_loan_amount: Optional[int]
    estimated_weekly_saving: Optional[int]
    estimated_monthly_saving: Optional[int]
    estimated_annual_saving: Optional[int]
    estimated_lifetime_saving: Optional[int]
    evidence_json: List[Dict[str, Any]]
    assumptions_json: Dict[str, Any]
    source_json: Dict[str, Any]


def _build_refi_draft(
    lead: Lead,
    *,
    value_mid: int,
    loan_amount: int,
    profile: str,
    signals: List[Dict[str, Any]],
    mortgage_profile: Optional[LeadMortgageProfile],
) -> Optional[_OpportunityDraft]:
    if loan_amount < 220_000 or value_mid < 450_000:
        return None
    ownership_years = _safe_float(getattr(lead, "ownership_duration_years", None))
    if ownership_years < 2 and not signals:
        return None
    current_rate = mortgage_profile.current_rate if mortgage_profile and mortgage_profile.current_rate else (ASSUMED_BAD_INVESTOR_RATE if profile == "investor" else ASSUMED_BAD_OWNER_OCC_RATE)
    market_rate = ASSUMED_REFI_MARKET_RATE
    monthly_now = _monthly_payment(loan_amount, current_rate)
    monthly_new = _monthly_payment(loan_amount, market_rate)
    monthly_saving = int(max(0, round(monthly_now - monthly_new)))
    if monthly_saving < 140:
        return None
    annual_saving = monthly_saving * 12
    weekly_saving = int(round(annual_saving / 52))
    lifetime_saving = annual_saving * 5
    signal_labels = [str(item.get("label") or item.get("signal") or item.get("reason") or "").strip() for item in signals]
    signal_labels = [label for label in signal_labels if label]
    story = _lead_story_sentence(lead, profile, value_mid, ownership_years)
    why_now = _sentence(f"{story} Comparable public lender pricing is better than the inferred stale-rate scenario on this property")
    if signal_labels:
        why_now = _sentence(f"{why_now} Existing lead signals include {', '.join(signal_labels[:3])}")
    reason = _sentence(
        f"The estimated repayment gap is about ${monthly_saving:,.0f} per month, or roughly ${weekly_saving:,.0f} per week, if the current loan is still sitting on an older rate"
    )
    next_action = _sentence("Lead with a refinance review and confirm the current lender, current rate, fixed or variable status, and the rough loan balance")
    evidence = _evidence_rows(lead, value_mid, loan_amount, profile, len(signals))
    evidence.append({"label": "Estimated saving", "value": f"${weekly_saving:,.0f}/week | ${monthly_saving:,.0f}/month"})
    if mortgage_profile and mortgage_profile.current_lender:
        evidence.append({"label": "Current lender on file", "value": _sentence(f"The current lender on file is {mortgage_profile.current_lender}")})
    if mortgage_profile and mortgage_profile.fixed_or_variable:
        evidence.append({"label": "Rate type on file", "value": _sentence(f"The current loan is marked as {mortgage_profile.fixed_or_variable}")})
    return _OpportunityDraft(
        opportunity_type="refinance_review",
        priority_score=min(98.0, 56.0 + monthly_saving / 18.0 + (8.0 if profile == "owner_occupier" else 5.0) + min(len(signals) * 4.0, 12.0)),
        headline=f"Estimated refi gap ${weekly_saving:,.0f}/week",
        reason_to_call=reason,
        why_now=why_now,
        next_best_action=next_action,
        best_call_window="Next outbound block",
        current_rate_estimate=current_rate,
        market_rate_estimate=market_rate,
        estimated_loan_amount=loan_amount,
        estimated_weekly_saving=weekly_saving,
        estimated_monthly_saving=monthly_saving,
        estimated_annual_saving=annual_saving,
        estimated_lifetime_saving=lifetime_saving,
        evidence_json=evidence,
        assumptions_json={
            "estimated_only": True,
            "assumed_current_rate": current_rate,
            "assumed_market_rate": market_rate,
            "remaining_term_years": 25,
            "not_credit_advice": True,
        },
        source_json={"source": "deterministic_inference", "profile": profile, "signal_count": len(signals)},
    )


def _build_equity_draft(lead: Lead, *, value_mid: int, loan_amount: int, profile: str) -> Optional[_OpportunityDraft]:
    ownership_years = _safe_float(getattr(lead, "ownership_duration_years", None))
    available_equity = max(0, value_mid - loan_amount)
    if ownership_years < 7 or available_equity < 250_000:
        return None
    story = _lead_story_sentence(lead, profile, value_mid, ownership_years)
    reason = _sentence(
        f"{story} The estimated equity buffer is about ${available_equity:,.0f}, which is large enough to justify a debt review or a structured cash-out conversation"
    )
    evidence = _evidence_rows(lead, value_mid, loan_amount, profile, 0)
    evidence.append({"label": "Estimated equity buffer", "value": f"${available_equity:,.0f}"})
    return _OpportunityDraft(
        opportunity_type="equity_review",
        priority_score=min(90.0, 45.0 + available_equity / 25_000.0 + min(ownership_years, 12.0)),
        headline=f"Estimated equity ${available_equity:,.0f}",
        reason_to_call=reason,
        why_now=_sentence("The combination of longer tenure and the current valuation band suggests that this lead may still have borrowing room even without selling"),
        next_best_action=_sentence("Use an equity-release angle only after confirming the loan balance, the intended use of funds, and the occupancy position"),
        best_call_window="After first contact confirmation",
        current_rate_estimate=None,
        market_rate_estimate=None,
        estimated_loan_amount=loan_amount,
        estimated_weekly_saving=None,
        estimated_monthly_saving=None,
        estimated_annual_saving=None,
        estimated_lifetime_saving=None,
        evidence_json=evidence,
        assumptions_json={"estimated_only": True, "equity_is_gross_not_net_of_full_debt": True},
        source_json={"source": "deterministic_inference", "profile": profile},
    )


def _build_smsf_draft(lead: Lead, *, value_mid: int, loan_amount: int, profile: str) -> Optional[_OpportunityDraft]:
    if profile != "investor":
        return None
    if value_mid < 700_000 or loan_amount < 260_000:
        return None
    ownership_years = _safe_float(getattr(lead, "ownership_duration_years", None))
    if ownership_years < 4:
        return None
    story = _lead_story_sentence(lead, profile, value_mid, ownership_years)
    reason = _sentence(
        f"{story} This looks more like an investor-style ownership pattern than a pure owner-occupier scenario, so it is worth probing for portfolio goals and SMSF interest"
    )
    evidence = _evidence_rows(lead, value_mid, loan_amount, profile, 0)
    evidence.append({"label": "SMSF gate", "value": "Only a discovery angle. Do not treat this as suitability or advice."})
    return _OpportunityDraft(
        opportunity_type="smsf_probe",
        priority_score=min(78.0, 40.0 + ownership_years * 3.5 + value_mid / 120_000.0),
        headline="Investor pattern worth SMSF probe",
        reason_to_call=reason,
        why_now=_sentence("The lead has enough value and tenure for a serious portfolio conversation, even if SMSF turns out not to fit"),
        next_best_action=_sentence("Ask about investment intent, the existing super structure, and whether another property is in view rather than pitching SMSF directly"),
        best_call_window="After refinance/equity questions",
        current_rate_estimate=None,
        market_rate_estimate=None,
        estimated_loan_amount=loan_amount,
        estimated_weekly_saving=None,
        estimated_monthly_saving=None,
        estimated_annual_saving=None,
        estimated_lifetime_saving=None,
        evidence_json=evidence,
        assumptions_json={"estimated_only": True, "not_personal_advice": True, "smsf_requires_specialist_review": True},
        source_json={"source": "deterministic_inference", "profile": profile},
    )


def _drafts_for_lead(lead: Lead, mortgage_profile: Optional[LeadMortgageProfile]) -> List[_OpportunityDraft]:
    profile = _owner_profile(lead)
    value_mid = _value_midpoint(lead)
    loan_amount = _loan_amount_from_profile(mortgage_profile, _estimated_loan_amount(lead, value_mid))
    signals = _parse_signal_list(getattr(lead, "refinance_signals", None))
    drafts: List[_OpportunityDraft] = []
    refi = _build_refi_draft(
        lead,
        value_mid=value_mid,
        loan_amount=loan_amount,
        profile=profile,
        signals=signals,
        mortgage_profile=mortgage_profile,
    )
    if refi:
        drafts.append(refi)
    equity = _build_equity_draft(lead, value_mid=value_mid, loan_amount=loan_amount, profile=profile)
    if equity:
        drafts.append(equity)
    smsf = _build_smsf_draft(lead, value_mid=value_mid, loan_amount=loan_amount, profile=profile)
    if smsf:
        drafts.append(smsf)
    return drafts


async def ensure_bank_registry_seeded(session: AsyncSession) -> int:
    existing = await session.execute(select(BankDataHolder.id))
    ids = set(existing.scalars().all())
    created = 0
    now_iso = _now_iso()
    for record in get_cdr_bank_registry():
        if str(record["id"]) in ids:
            continue
        session.add(BankDataHolder(created_at=now_iso, updated_at=now_iso, **record))
        created += 1
    if created:
        await session.commit()
    return created


async def list_bank_data_holders(session: AsyncSession) -> List[BankDataHolder]:
    await ensure_bank_registry_seeded(session)
    rows = await session.execute(
        select(BankDataHolder).where(BankDataHolder.active == True).order_by(BankDataHolder.name.asc())  # noqa: E712
    )
    return list(rows.scalars().all())


async def recompute_mortgage_opportunities(session: AsyncSession, lead_id: Optional[str] = None) -> Dict[str, Any]:
    now_iso = _now_iso()
    expiry = (datetime.now(SYDNEY_TZ) + timedelta(days=ACTIVE_OPPORTUNITY_WINDOW_DAYS)).replace(microsecond=0).isoformat()
    statement = select(Lead)
    if lead_id:
        statement = statement.where(Lead.id == lead_id)
    leads = list((await session.execute(statement)).scalars().all())
    if lead_id and not leads:
        return {"processed": 0, "created": 0, "lead_ids": []}

    created = 0
    lead_ids = [lead.id for lead in leads]
    if lead_ids:
        await session.execute(delete(MortgageOpportunity).where(MortgageOpportunity.lead_id.in_(lead_ids)))
    for lead in leads:
        mortgage_profile = await session.get(LeadMortgageProfile, lead.id)
        active_feedback = await list_active_feedback(session, lead.id)
        suburb_check = suburb_cross_check_summary(lead)
        market_snapshot = await best_market_rate(
            session,
            occupancy_target="investor" if _owner_profile(lead) == "investor" else "owner_occupier",
            rate_type="variable",
        )
        drafts = _drafts_for_lead(lead, mortgage_profile)
        for draft in drafts:
            if should_suppress_opportunity(active_feedback, draft.opportunity_type):
                continue
            assumptions_json = dict(draft.assumptions_json)
            source_json = dict(draft.source_json)
            market_rate = draft.market_rate_estimate
            evidence_json = list(draft.evidence_json)
            priority_score = draft.priority_score
            why_now = draft.why_now
            reason_to_call = draft.reason_to_call
            if suburb_check.get("status") == "mismatch":
                priority_score = max(18.0, priority_score - 18.0)
                evidence_json.append({"label": "Suburb cross-check", "value": _sentence(str(suburb_check["reason"]))})
                why_now = _sentence(f"{why_now} The nearby-sale cross-check is not fully aligned, so the valuation band is being treated with caution")
            elif suburb_check.get("status") == "confirmed":
                evidence_json.append({"label": "Suburb cross-check", "value": _sentence(str(suburb_check["reason"]))})
            if mortgage_profile and mortgage_profile.current_lender and draft.opportunity_type == "refinance_review":
                reason_to_call = _sentence(f"{reason_to_call} The current lender on file is {mortgage_profile.current_lender}")
            balance_band = profile_balance_band(mortgage_profile)
            if balance_band:
                evidence_json.append({"label": "Balance band", "value": _sentence(f"The working balance band is {balance_band}")})
            if market_snapshot.get("rate") is not None and draft.opportunity_type == "refinance_review":
                market_rate = float(market_snapshot["rate"])
                if draft.estimated_loan_amount:
                    current_rate = draft.current_rate_estimate or ASSUMED_BAD_OWNER_OCC_RATE
                    monthly_now = _monthly_payment(draft.estimated_loan_amount, current_rate)
                    monthly_new = _monthly_payment(draft.estimated_loan_amount, market_rate)
                    monthly_saving = int(max(0, round(monthly_now - monthly_new)))
                    annual_saving = monthly_saving * 12
                    weekly_saving = int(round(annual_saving / 52))
                    lifetime_saving = annual_saving * 5
                else:
                    monthly_saving = draft.estimated_monthly_saving
                    annual_saving = draft.estimated_annual_saving
                    weekly_saving = draft.estimated_weekly_saving
                    lifetime_saving = draft.estimated_lifetime_saving
                source_json["market_snapshot"] = market_snapshot
                assumptions_json["market_rate_source"] = "stored_lender_products"
                if market_snapshot.get("lender_name") and market_snapshot.get("product_name"):
                    evidence_json.append(
                        {
                            "label": "Best stored market product",
                            "value": f"{market_snapshot['lender_name']} | {market_snapshot['product_name']} | {market_rate:.2f}%",
                        }
                    )
            else:
                monthly_saving = draft.estimated_monthly_saving
                annual_saving = draft.estimated_annual_saving
                weekly_saving = draft.estimated_weekly_saving
                lifetime_saving = draft.estimated_lifetime_saving
            session.add(
                MortgageOpportunity(
                    id=str(uuid.uuid4()),
                    lead_id=lead.id,
                    opportunity_type=draft.opportunity_type,
                    status="active",
                    priority_score=round(priority_score, 2),
                    headline=draft.headline,
                    reason_to_call=reason_to_call,
                    why_now=why_now,
                    next_best_action=draft.next_best_action,
                    best_call_window=draft.best_call_window,
                    estimated_loan_amount=draft.estimated_loan_amount,
                    current_rate_estimate=draft.current_rate_estimate,
                    market_rate_estimate=market_rate,
                    estimated_weekly_saving=weekly_saving,
                    estimated_monthly_saving=monthly_saving,
                    estimated_annual_saving=annual_saving,
                    estimated_lifetime_saving=lifetime_saving,
                    evidence_json=evidence_json,
                    assumptions_json=assumptions_json,
                    source_json=source_json,
                    created_at=now_iso,
                    updated_at=now_iso,
                    expires_at=expiry,
                )
            )
            created += 1
    await session.commit()
    return {"processed": len(leads), "created": created, "lead_ids": lead_ids}


async def get_lead_mortgage_opportunities(session: AsyncSession, lead_id: str, auto_recompute: bool = True) -> List[MortgageOpportunity]:
    rows = await session.execute(
        select(MortgageOpportunity)
        .where(MortgageOpportunity.lead_id == lead_id)
        .order_by(MortgageOpportunity.priority_score.desc(), MortgageOpportunity.updated_at.desc())
    )
    opportunities = list(rows.scalars().all())
    if opportunities or not auto_recompute:
        return opportunities
    await recompute_mortgage_opportunities(session, lead_id=lead_id)
    rows = await session.execute(
        select(MortgageOpportunity)
        .where(MortgageOpportunity.lead_id == lead_id)
        .order_by(MortgageOpportunity.priority_score.desc(), MortgageOpportunity.updated_at.desc())
    )
    return list(rows.scalars().all())


async def summarize_mortgage_coverage(session: AsyncSession) -> Dict[str, Any]:
    await ensure_bank_registry_seeded(session)
    lender_count = _safe_int((await session.execute(text("SELECT COUNT(*) FROM bank_data_holders WHERE active = true"))).scalar_one())
    product_count = _safe_int((await session.execute(text("SELECT COUNT(*) FROM lender_products"))).scalar_one())
    opportunity_count = _safe_int((await session.execute(text("SELECT COUNT(*) FROM mortgage_opportunities WHERE status = 'active'"))).scalar_one())
    refi_count = _safe_int(
        (
            await session.execute(
                text("SELECT COUNT(*) FROM mortgage_opportunities WHERE status = 'active' AND opportunity_type = 'refinance_review'")
            )
        ).scalar_one()
    )
    return {
        "lender_count": lender_count,
        "product_count": product_count,
        "active_opportunity_count": opportunity_count,
        "refinance_opportunity_count": refi_count,
        "assumptions": {
            "market_refi_rate": ASSUMED_REFI_MARKET_RATE,
            "owner_occupier_bad_rate": ASSUMED_BAD_OWNER_OCC_RATE,
            "investor_bad_rate": ASSUMED_BAD_INVESTOR_RATE,
            "fixed_review_rate": ASSUMED_FIXED_REVIEW_RATE,
        },
    }
