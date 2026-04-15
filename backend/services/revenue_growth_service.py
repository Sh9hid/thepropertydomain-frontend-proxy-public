from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

from sqlalchemy import case, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_core_models import ContactAttempt, ContentAsset, LeadContact, LeadState, TaskQueue
from models.schemas import SendEmailRequest
from models.sql_models import Lead
from services.integrations import send_email_service
from services.sales_core.dialing_service import get_lead_context, sync_lead_state


BASIC_SEQUENCE_KEY = "basic_outreach"
DEFAULT_VARIANTS = ("market_intel", "credibility", "direct_offer")

BUSINESS_CONTEXT_STRATEGIES: Dict[str, Dict[str, Any]] = {
    "real_estate": {
        "label": "Real Estate",
        "channel_bias": "call_heavy",
        "sms_supported": True,
        "default_variant": "market_intel",
        "cta": "If it helps, I can send over a quick market snapshot or line up a short call.",
    },
    "mortgage": {
        "label": "Mortgage",
        "channel_bias": "mixed",
        "sms_supported": False,
        "default_variant": "credibility",
        "cta": "If useful, I can outline the likely refinance options in a quick call.",
    },
    "app_saas": {
        "label": "App / SaaS",
        "channel_bias": "email_first",
        "sms_supported": False,
        "default_variant": "direct_offer",
        "cta": "If relevant, I can send a tighter teardown with the next few growth moves.",
    },
}

SEQUENCE_STEPS: tuple[dict[str, Any], ...] = (
    {"step": 0, "day_offset": 0, "task_type": "sequence_email", "channel": "email", "title": "Initial outreach email"},
    {"step": 1, "day_offset": 1, "task_type": "sequence_email", "channel": "email", "title": "First follow-up email"},
    {"step": 2, "day_offset": 3, "task_type": "sequence_call", "channel": "call", "title": "Call follow-up"},
    {"step": 3, "day_offset": 5, "task_type": "sequence_email", "channel": "email", "title": "Final follow-up email"},
)


def _resolved_now(now: Optional[datetime] = None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    return (now if now.tzinfo else now.replace(tzinfo=timezone.utc)).replace(microsecond=0)


def get_business_context_strategy(business_context_key: str) -> Dict[str, Any]:
    return {
        "label": business_context_key.replace("_", " ").title(),
        "channel_bias": "mixed",
        "sms_supported": False,
        "default_variant": "market_intel",
        "cta": "If it makes sense, I can send a short note with the next best step.",
        **BUSINESS_CONTEXT_STRATEGIES.get((business_context_key or "").strip().lower(), {}),
    }


def _safe_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _serialize_lead(lead: Optional[Lead]) -> Dict[str, Any]:
    if lead is None:
        return {}
    if hasattr(lead, "model_dump"):
        return lead.model_dump()
    return dict(lead)


def _lead_context_line(lead: Dict[str, Any]) -> str:
    address = str(lead.get("address") or "").strip()
    suburb = str(lead.get("suburb") or "").strip()
    property_type = str(lead.get("property_type") or "").strip()
    if address and suburb:
        return f"{address}, {suburb}"
    if address:
        return address
    if suburb and property_type:
        return f"{property_type} activity in {suburb}"
    if suburb:
        return f"activity in {suburb}"
    return "your market"


def _personalization_points(contact: LeadContact, lead: Dict[str, Any]) -> list[str]:
    points: list[str] = []
    lead_context = _lead_context_line(lead)
    if lead_context:
        points.append(f"I was looking at {lead_context}.")

    suburb = str(lead.get("suburb") or "").strip()
    estimated_value_low = lead.get("estimated_value_low")
    estimated_value_high = lead.get("estimated_value_high")
    if suburb and estimated_value_low and estimated_value_high:
        points.append(
            f"The current valuation range around {suburb} looks to be roughly ${int(estimated_value_low):,}-${int(estimated_value_high):,}."
        )
    elif suburb and lead.get("estimated_value_high"):
        points.append(f"There is still active movement around comparable properties in {suburb}.")

    signals = _safe_json_list(lead.get("seller_intent_signals")) + _safe_json_list(lead.get("refinance_signals"))
    if signals:
        top_signal = signals[0]
        if isinstance(top_signal, dict):
            signal_label = str(top_signal.get("label") or top_signal.get("type") or top_signal.get("signal") or "").strip()
            if signal_label:
                points.append(f"One of the stronger signals on the record is {signal_label.lower()}.")

    metadata = contact.metadata_json or {}
    property_hint = str(metadata.get("property_note") or metadata.get("suburb_note") or "").strip()
    if property_hint:
        points.append(property_hint)

    return points[:3]


def _build_subject(contact: LeadContact, lead: Dict[str, Any], variant_key: str, step: int) -> str:
    name = (contact.full_name or lead.get("owner_name") or "there").split()[0]
    context_line = _lead_context_line(lead)
    if step == 0:
        if variant_key == "direct_offer":
            return f"{name}, quick idea for {context_line}"
        if variant_key == "credibility":
            return f"Quick note on {context_line}"
        return f"Market signal on {context_line}"
    if step == 1:
        return f"Following up on {context_line}"
    if step == 3:
        return f"Final note on {context_line}"
    return f"Next step for {context_line}"


def _build_body(contact: LeadContact, lead: Dict[str, Any], business_context_key: str, variant_key: str, step: int) -> str:
    strategy = get_business_context_strategy(business_context_key)
    name = (contact.full_name or lead.get("owner_name") or "there").split()[0]
    intro = f"Hi {name},"
    points = _personalization_points(contact, lead)
    hook = {
        "market_intel": "I wanted to send a short note because the recent signals on the record looked worth a closer look.",
        "credibility": "I spend a lot of time working these accounts and thought the current position looked worth a direct note.",
        "direct_offer": "I think there is a simple next move here if the timing is right.",
    }.get(variant_key, "I wanted to send a quick note.")

    if step == 1:
        hook = "Just following up in case my earlier note got buried."
    elif step == 3:
        hook = "Closing the loop with one last note."

    middle = " ".join(points) if points else "I can keep it brief and focused on what matters next."
    outro = strategy["cta"]
    if step == 2:
        outro = "This one is best handled as a call task rather than another email."

    return "\n\n".join([intro, hook, middle, outro]).strip()


async def _load_contact_with_lead(session: AsyncSession, lead_contact_id: str) -> tuple[LeadContact, Optional[Lead]]:
    contact = await session.get(LeadContact, lead_contact_id)
    if contact is None:
        raise ValueError(f"Lead contact not found: {lead_contact_id}")
    lead = await session.get(Lead, contact.lead_id) if contact.lead_id else None
    return contact, lead


async def _load_email_account(session: AsyncSession, account_id: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        if account_id:
            row = (
                await session.execute(text("SELECT * FROM email_accounts WHERE id = :id LIMIT 1"), {"id": account_id})
            ).mappings().first()
            if row:
                return dict(row)
        row = (
            await session.execute(
                text("SELECT * FROM email_accounts WHERE COALESCE(is_active, 1) = 1 ORDER BY updated_at DESC, created_at DESC LIMIT 1")
            )
        ).mappings().first()
        return dict(row) if row else None
    except Exception:
        return None


async def _select_best_variant(session: AsyncSession, business_context_key: str) -> str:
    rows = (
        await session.execute(
            select(
                ContactAttempt.variant_key,
                func.count(ContactAttempt.id).label("send_count"),
                func.sum(case((ContactAttempt.opened_at.is_not(None), 1), else_=0)).label("open_count"),
                func.sum(case((ContactAttempt.replied_at.is_not(None), 1), else_=0)).label("reply_count"),
            )
            .where(ContactAttempt.business_context_key == business_context_key)
            .where(ContactAttempt.channel == "email")
            .where(ContactAttempt.variant_key.is_not(None))
            .group_by(ContactAttempt.variant_key)
        )
    ).all()
    if not rows:
        return get_business_context_strategy(business_context_key)["default_variant"]

    best_variant = None
    best_score = float("-inf")
    for variant_key, send_count, open_count, reply_count in rows:
        sends = int(send_count or 0)
        if sends <= 0:
            continue
        opens = int(open_count or 0)
        replies = int(reply_count or 0)
        score = (replies / sends) * 100.0 + (opens / sends) * 10.0
        if score > best_score:
            best_score = score
            best_variant = str(variant_key or "")
    return best_variant or get_business_context_strategy(business_context_key)["default_variant"]


async def queue_basic_sequence(
    session: AsyncSession,
    *,
    business_context_key: str,
    lead_contact_id: str,
    start_at: Optional[datetime] = None,
    created_by: str = "system",
    initial_attempt_id: Optional[str] = None,
) -> list[TaskQueue]:
    resolved_now = _resolved_now(start_at)
    contact, _ = await _load_contact_with_lead(session, lead_contact_id)

    await session.execute(
        delete(TaskQueue)
        .where(TaskQueue.lead_contact_id == lead_contact_id)
        .where(TaskQueue.status == "pending")
        .where(TaskQueue.task_type.in_(["sequence_email", "sequence_call"]))
    )

    queued: list[TaskQueue] = []
    for step in SEQUENCE_STEPS[1:]:
        due_at = resolved_now + timedelta(days=int(step["day_offset"]))
        task = TaskQueue(
            business_context_key=business_context_key,
            lead_contact_id=lead_contact_id,
            task_type=str(step["task_type"]),
            due_at=due_at,
            status="pending",
            priority=90 - int(step["step"]) * 5,
            reason=str(step["title"]),
            payload_json={
                "sequence_key": BASIC_SEQUENCE_KEY,
                "sequence_step": int(step["step"]),
                "channel": step["channel"],
                "title": step["title"],
                "initial_attempt_id": initial_attempt_id,
                "sms_supported": get_business_context_strategy(contact.business_context_key).get("sms_supported", False),
            },
            created_by=created_by,
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(task)
        queued.append(task)

    await session.commit()
    for task in queued:
        await session.refresh(task)
    return queued


async def send_outreach_email(
    session: AsyncSession,
    *,
    business_context_key: str,
    lead_contact_id: str,
    sequence_step: int = 0,
    account_id: Optional[str] = None,
    force_variant: Optional[str] = None,
    created_by: str = "system",
    now: Optional[datetime] = None,
    queue_follow_ups: bool = True,
    send_fn: Optional[Callable[[Optional[Dict[str, Any]], SendEmailRequest], None]] = None,
) -> Dict[str, Any]:
    resolved_now = _resolved_now(now)
    contact, lead_row = await _load_contact_with_lead(session, lead_contact_id)
    lead = _serialize_lead(lead_row)

    recipient_email = str(contact.primary_email or lead.get("contact_emails", [""])[0] if lead.get("contact_emails") else contact.primary_email or "").strip()
    if not recipient_email:
        raise ValueError("Lead contact does not have an email address")

    variant_key = force_variant or await _select_best_variant(session, business_context_key)
    subject = _build_subject(contact, lead, variant_key, sequence_step)
    body = _build_body(contact, lead, business_context_key, variant_key, sequence_step)

    email_request = SendEmailRequest(
        account_id=account_id or "default",
        recipient=recipient_email,
        subject=subject,
        body=body,
        plain_text=True,
    )
    account_data = await _load_email_account(session, account_id)
    sender = send_fn or send_email_service
    await asyncio.to_thread(sender, account_data, email_request)

    attempt = ContactAttempt(
        business_context_key=business_context_key,
        lead_contact_id=lead_contact_id,
        attempted_at=resolved_now,
        channel="email",
        outcome="sent",
        connected=False,
        duration_seconds=0,
        voicemail_left=False,
        note=f"Sequence email step {sequence_step}",
        recipient_email=recipient_email,
        email_subject=subject,
        email_body=body,
        sequence_key=BASIC_SEQUENCE_KEY,
        sequence_step=sequence_step,
        variant_key=variant_key,
        performance_json={"open_count": 0, "reply_count": 0},
        created_by=created_by,
        created_at=resolved_now,
    )
    session.add(attempt)
    await session.flush()

    if lead_row is not None:
        lead_row.last_contacted_at = resolved_now.isoformat()
        lead_row.last_outbound_at = resolved_now.isoformat()
        lead_row.last_activity_type = "email"
        lead_row.cadence_name = BASIC_SEQUENCE_KEY
        lead_row.cadence_step = sequence_step
        lead_row.preferred_channel = "email"
        lead_row.updated_at = resolved_now.isoformat()

    queued_tasks = []
    if queue_follow_ups and sequence_step == 0:
        queued_tasks = await queue_basic_sequence(
            session,
            business_context_key=business_context_key,
            lead_contact_id=lead_contact_id,
            start_at=resolved_now,
            created_by=created_by,
            initial_attempt_id=attempt.id,
        )
    else:
        await session.commit()

    state = await sync_lead_state(session, lead_contact_id, now=resolved_now)
    await session.refresh(attempt)
    context = await get_lead_context(session, lead_contact_id)
    return {
        "attempt": attempt,
        "state": state,
        "tasks": queued_tasks,
        "contact": context["contact"],
        "lead": context["lead"],
    }


async def record_email_event(
    session: AsyncSession,
    *,
    attempt_id: str,
    event_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> ContactAttempt:
    resolved_now = _resolved_now(now)
    attempt = await session.get(ContactAttempt, attempt_id)
    if attempt is None:
        raise ValueError(f"Contact attempt not found: {attempt_id}")
    if attempt.channel != "email":
        raise ValueError("Only email attempts can record email events")

    payload = dict(attempt.performance_json or {})
    payload.setdefault("events", [])
    payload["events"].append({"type": event_type, "at": resolved_now.isoformat(), "metadata": metadata or {}})
    payload["open_count"] = int(payload.get("open_count") or 0)
    payload["reply_count"] = int(payload.get("reply_count") or 0)

    normalized = (event_type or "").strip().lower()
    if normalized == "open":
        if attempt.opened_at is None:
            attempt.opened_at = resolved_now
        payload["open_count"] += 1
    elif normalized == "reply":
        if attempt.replied_at is None:
            attempt.replied_at = resolved_now
        payload["reply_count"] += 1
        attempt.outcome = "replied"
    else:
        payload[f"{normalized}_count"] = int(payload.get(f"{normalized}_count") or 0) + 1

    attempt.performance_json = payload
    await session.commit()
    await session.refresh(attempt)
    return attempt


async def summarize_email_performance(session: AsyncSession, *, business_context_key: str) -> Dict[str, Any]:
    rows = (
        await session.execute(
            select(ContactAttempt.variant_key, ContactAttempt.opened_at, ContactAttempt.replied_at)
            .where(ContactAttempt.business_context_key == business_context_key)
            .where(ContactAttempt.channel == "email")
        )
    ).all()
    summary: Dict[str, Any] = {"send_count": len(rows), "open_count": 0, "reply_count": 0, "best_variant": None}
    variant_stats: Dict[str, Dict[str, int]] = {}
    for variant_key, opened_at, replied_at in rows:
        variant = str(variant_key or "unknown")
        variant_stats.setdefault(variant, {"send_count": 0, "open_count": 0, "reply_count": 0})
        variant_stats[variant]["send_count"] += 1
        if opened_at:
            summary["open_count"] += 1
            variant_stats[variant]["open_count"] += 1
        if replied_at:
            summary["reply_count"] += 1
            variant_stats[variant]["reply_count"] += 1

    best_variant = None
    best_score = float("-inf")
    for variant, stats in variant_stats.items():
        sends = stats["send_count"] or 1
        score = (stats["reply_count"] / sends) * 100.0 + (stats["open_count"] / sends) * 10.0
        if score > best_score:
            best_variant = variant
            best_score = score
    summary["best_variant"] = best_variant
    summary["variants"] = variant_stats
    return summary


async def _collect_content_source_material(session: AsyncSession, limit: int = 20) -> Dict[str, Any]:
    rows = (
        await session.execute(
            text(
                """
                SELECT call_log.id,
                       call_log.lead_id,
                       COALESCE(call_log.transcript, '') AS transcript,
                       COALESCE(call_log.summary, '') AS summary,
                       COALESCE(call_log.objection_tags, '[]') AS objection_tags,
                       COALESCE(leads.suburb, '') AS suburb,
                       COALESCE(leads.address, '') AS address
                FROM call_log
                LEFT JOIN leads ON leads.id = call_log.lead_id
                WHERE COALESCE(call_log.transcript, '') != '' OR COALESCE(call_log.summary, '') != ''
                ORDER BY COALESCE(call_log.logged_at, call_log.timestamp, '') DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        )
    ).mappings().all()

    objection_counter: Counter[str] = Counter()
    transcript_snippets: list[str] = []
    suburbs: Counter[str] = Counter()
    for row in rows:
        transcript = str(row.get("transcript") or row.get("summary") or "").strip()
        if transcript:
            transcript_snippets.append(transcript[:280])
        for tag in _safe_json_list(row.get("objection_tags")):
            objection_counter[str(tag)] += 1
        suburb = str(row.get("suburb") or "").strip()
        if suburb:
            suburbs[suburb] += 1

    return {
        "transcript_snippets": transcript_snippets[:8],
        "top_objections": [tag for tag, _ in objection_counter.most_common(5)],
        "top_suburbs": [suburb for suburb, _ in suburbs.most_common(3)],
        "source_count": len(rows),
    }


def _fallback_content_text(asset_type: str, index: int, source: Dict[str, Any], business_context_key: str) -> tuple[str, str]:
    objections = ", ".join(source.get("top_objections") or ["timing", "price", "uncertainty"])
    suburbs = ", ".join(source.get("top_suburbs") or ["the local market"])
    snippet = (source.get("transcript_snippets") or ["Clients are asking for clearer next steps."])[0]
    if asset_type == "linkedin_post":
        title = f"LinkedIn post {index + 1}"
        body = (
            f"Teams in {suburbs} keep hitting the same objections: {objections}. "
            f"One recent transcript line that stands out: \"{snippet}\" "
            f"The fix is usually a clearer next step and tighter proof, not a longer pitch."
        )
        return title, body
    if asset_type == "blog":
        title = "From objections to pipeline: what calls are really saying"
        body = (
            f"Call transcripts across {business_context_key} keep circling the same blockers: {objections}. "
            f"This draft blog turns those objections into a practical response framework, using examples from {suburbs} and real operator language."
        )
        return title, body
    title = "Weekly newsletter draft"
    body = (
        f"This week's signal stack: the loudest objections were {objections}. "
        f"The main takeaway from recent calls is simple: \"{snippet}\" "
        f"If the team tightens follow-up discipline, the next sequence should convert better."
    )
    return title, body


async def generate_content_assets(
    session: AsyncSession,
    *,
    business_context_key: str,
    posts_per_day: int = 5,
    blog_count: int = 1,
    newsletter_count: int = 1,
    created_by: str = "system",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    resolved_now = _resolved_now(now)
    source = await _collect_content_source_material(session)
    assets: list[ContentAsset] = []

    for index in range(posts_per_day):
        title, content_text = _fallback_content_text("linkedin_post", index, source, business_context_key)
        asset = ContentAsset(
            business_context_key=business_context_key,
            asset_type="linkedin_post",
            title=title,
            content_text=content_text,
            variant_key="daily_batch",
            source_json=source,
            created_by=created_by,
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(asset)
        assets.append(asset)

    for _ in range(blog_count):
        title, content_text = _fallback_content_text("blog", 0, source, business_context_key)
        asset = ContentAsset(
            business_context_key=business_context_key,
            asset_type="blog",
            title=title,
            content_text=content_text,
            variant_key="daily_batch",
            source_json=source,
            created_by=created_by,
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(asset)
        assets.append(asset)

    for _ in range(newsletter_count):
        title, content_text = _fallback_content_text("newsletter", 0, source, business_context_key)
        asset = ContentAsset(
            business_context_key=business_context_key,
            asset_type="newsletter",
            title=title,
            content_text=content_text,
            variant_key="daily_batch",
            source_json=source,
            created_by=created_by,
            created_at=resolved_now,
            updated_at=resolved_now,
        )
        session.add(asset)
        assets.append(asset)

    await session.commit()
    for asset in assets:
        await session.refresh(asset)

    return {
        "generated_at": resolved_now.isoformat(),
        "source": source,
        "assets": assets,
    }


async def generate_daily_content_bundle(
    session: AsyncSession,
    *,
    business_context_key: str,
    run_date: Optional[date] = None,
    posts_per_day: int = 5,
    blog_count: int = 1,
    newsletter_count: int = 1,
    created_by: str = "system",
) -> Dict[str, Any]:
    payload = await generate_content_assets(
        session,
        business_context_key=business_context_key,
        posts_per_day=posts_per_day,
        blog_count=blog_count,
        newsletter_count=newsletter_count,
        created_by=created_by,
        now=datetime.combine(run_date or date.today(), datetime.min.time(), tzinfo=timezone.utc),
    )
    return {
        "run_date": (run_date or date.today()).isoformat(),
        "counts": dict(Counter(asset.asset_type for asset in payload["assets"])),
        "assets": payload["assets"],
        "source": payload["source"],
    }
