"""Background loops — all imports are boot-safe with try/except fallbacks."""
from __future__ import annotations

import asyncio
import logging as _logging
from datetime import datetime as _datetime
from typing import Any, Dict

from core.config import ALL_TARGET_SUBURBS, DOOR_KNOCK_SYNC_POLL_SECONDS, SYDNEY_TZ

_loop_log = _logging.getLogger(__name__)
_DA_POSTCODES = ["2765", "2517", "2518", "2756", "2775"]

# ---------- heartbeat tracking ----------
_heartbeats: Dict[str, Dict[str, Any]] = {}


def _beat(name: str, status: str = "healthy", detail: str = "") -> None:
    from core.utils import now_iso
    _heartbeats[name] = {"last_heartbeat": now_iso(), "status": status, "detail": detail}


def record_loop_heartbeat(name: str, status: str = "healthy", detail: str = "") -> None:
    _beat(name, status, detail)


def get_loop_heartbeats() -> Dict[str, str]:
    """Backward-compatible shape for API routes: loop_name -> ISO timestamp."""
    payload: Dict[str, str] = {}
    for loop_name, meta in _heartbeats.items():
        value = meta.get("last_heartbeat")
        if isinstance(value, str) and value:
            payload[loop_name] = value
    return payload


def get_loop_heartbeat_details() -> Dict[str, Dict[str, Any]]:
    return dict(_heartbeats)


# ---------- boot-safe imports ----------
def _get_session_factory():
    from core.database import _async_session_factory
    return _async_session_factory


async def _domain_ingest_loop():
    while True:
        try:
            _beat("domain_ingest", "running")
            from services.domain_withdrawn import ingest_withdrawn_to_leads
            from services.stale_active_listings import ingest_stale_active_to_leads
            session_factory = _get_session_factory()
            async with session_factory() as session:
                res_w = await ingest_withdrawn_to_leads(session, ALL_TARGET_SUBURBS)
                _loop_log.info("Domain withdrawn: %s", res_w)
                res_s = await ingest_stale_active_to_leads(session, ALL_TARGET_SUBURBS, days_min=70)
                _loop_log.info("Domain stale active: %s", res_s)
            _beat("domain_ingest", "healthy")
        except Exception as exc:
            _loop_log.warning("Domain ingest loop error: %s", exc)
            _beat("domain_ingest", "error", str(exc))
        await asyncio.sleep(4 * 3600)


async def _da_feed_loop():
    while True:
        try:
            now = _datetime.now(SYDNEY_TZ)
            target = now.replace(hour=7, minute=30, second=0, microsecond=0)
            if now >= target:
                from datetime import timedelta
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())
            _beat("da_feed", "running")
            from services.da_feed_ingestor import ingest_das_to_leads
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await ingest_das_to_leads(session, _DA_POSTCODES, days_back=3)
                _loop_log.info("DA feed ingestor: %s", result)
            _beat("da_feed", "healthy")
        except Exception as exc:
            _loop_log.warning("DA feed loop error: %s", exc)
            _beat("da_feed", "error", str(exc))
        await asyncio.sleep(3600)


async def _domain_enrichment_loop():
    while True:
        try:
            if 7 <= _datetime.now(SYDNEY_TZ).hour < 22:
                _beat("domain_enrichment", "running")
                from services.domain_enrichment import run_enrichment_batch
                session_factory = _get_session_factory()
                async with session_factory() as session:
                    result = await run_enrichment_batch(session)
                    if result.get("enriched", 0) > 0:
                        _loop_log.info("Domain enrichment: %s", result)
                _beat("domain_enrichment", "healthy")
        except Exception as exc:
            _loop_log.warning("Domain enrichment loop error: %s", exc)
            _beat("domain_enrichment", "error", str(exc))
        await asyncio.sleep(3600)


async def _geocoding_loop():
    while True:
        try:
            if 7 <= _datetime.now(SYDNEY_TZ).hour < 22:
                _beat("geocoding", "running")
                from services.geocoding_service import run_geocoding_batch
                session_factory = _get_session_factory()
                async with session_factory() as session:
                    result = await run_geocoding_batch(session, limit=20)
                    if result.get("geocoded", 0) > 0:
                        _loop_log.info("Lead geocoding: %s", result)
                _beat("geocoding", "healthy")
        except Exception as exc:
            _loop_log.warning("Geocoding loop error: %s", exc)
            _beat("geocoding", "error", str(exc))
        await asyncio.sleep(3600)


async def _distress_source_loop(source_key: str, cadence_minutes: int):
    await asyncio.sleep(30)
    while True:
        try:
            _beat(f"distress_{source_key}", "running")
            from services.distress_intel_service import run_distress_source
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await run_distress_source(session, source_key)
                _loop_log.info("[distress] %s: %s", source_key, result)
            _beat(f"distress_{source_key}", "healthy")
        except Exception as exc:
            _loop_log.warning("[distress] %s loop error: %s", source_key, exc)
            _beat(f"distress_{source_key}", "error", str(exc))
        await asyncio.sleep(cadence_minutes * 60)


async def _auto_ticket_loop():
    await asyncio.sleep(120)
    while True:
        try:
            _beat("auto_ticket", "running")
            from services.auto_ticket_watcher import run_auto_ticket_watcher
            session_factory = _get_session_factory()
            async with session_factory() as session:
                await run_auto_ticket_watcher(session)
            _beat("auto_ticket", "healthy")
        except Exception as exc:
            _loop_log.warning("Auto-ticket watcher error: %s", exc)
            _beat("auto_ticket", "error", str(exc))
        await asyncio.sleep(30 * 60)


async def _probate_scraper_loop():
    await asyncio.sleep(60)
    while True:
        try:
            _beat("probate_scraper", "running")
            from services.probate_scraper import scrape_nsw_probate_market
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await scrape_nsw_probate_market(session)
                _loop_log.info("Probate scraper: %s", result)
            _beat("probate_scraper", "healthy")
        except Exception as exc:
            _loop_log.warning("Probate scraper loop error: %s", exc)
            _beat("probate_scraper", "error", str(exc))
        await asyncio.sleep(24 * 3600)


async def _hermes_department_scheduler_loop():
    await asyncio.sleep(300)
    _loop_log.info("[HERMES Scheduler] Department cycle scheduler started")
    workspace_cadence = {
        "real_estate": 4 * 3600,
        "mortgage": 6 * 3600,
        "software": 8 * 3600,
        "shared": 12 * 3600,
    }
    last_run: dict = {}
    while True:
        try:
            _beat("hermes_scheduler", "running")
            from hermes.department_runner import run_department_cycle
            from hermes.departments import list_all_departments
            session_factory = _get_session_factory()
            all_depts = list_all_departments()
            now_ts = _datetime.now(SYDNEY_TZ).timestamp()
            for dept in all_depts:
                dept_id = dept["id"]
                workspace = dept["workspace"]
                cadence = workspace_cadence.get(workspace, 6 * 3600)
                last = last_run.get(dept_id, 0)
                if now_ts - last >= cadence:
                    try:
                        async with session_factory() as session:
                            result = await run_department_cycle(dept_id, session)
                            if result.get("status") == "complete":
                                last_run[dept_id] = now_ts
                    except Exception as dept_exc:
                        _loop_log.debug("[HERMES Scheduler] %s error: %s", dept_id, dept_exc)
                    await asyncio.sleep(30)
            _beat("hermes_scheduler", "healthy")
        except Exception as exc:
            _loop_log.warning("[HERMES Scheduler] cycle error: %s", exc)
            _beat("hermes_scheduler", "error", str(exc))
        await asyncio.sleep(300)


async def _research_scheduler_loop():
    await asyncio.sleep(180)
    while True:
        try:
            now = _datetime.now(SYDNEY_TZ)
            target = now.replace(hour=7, minute=0, second=0, microsecond=0)
            if now >= target:
                from datetime import timedelta
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())
            _beat("research_scheduler", "running")
            from models.org_models import ResearchArea
            from services.research_service import run_research
            session_factory = _get_session_factory()
            async with session_factory() as session:
                for area in (ResearchArea.SALES, ResearchArea.REAL_ESTATE, ResearchArea.APP_TECH):
                    await run_research(session, area)
            _beat("research_scheduler", "healthy")
        except Exception as exc:
            _loop_log.warning("Research scheduler error: %s", exc)
            _beat("research_scheduler", "error", str(exc))
        await asyncio.sleep(3600)


async def _door_knock_excel_sync_loop():
    await asyncio.sleep(20)
    while True:
        try:
            _beat("door_knock_excel_sync", "running")
            from services.door_knock_sync_service import run_door_knock_sheet_sync_once, sync_enabled

            if sync_enabled():
                session_factory = _get_session_factory()
                async with session_factory() as session:
                    result = await run_door_knock_sheet_sync_once(session)
                    if result.get("imported", 0) or result.get("exported", 0):
                        _loop_log.info("[door_knock_sync] %s", result)
                _beat("door_knock_excel_sync", "healthy")
            else:
                _beat("door_knock_excel_sync", "disabled", "DOOR_KNOCK_SYNC_FILE not configured")
        except Exception as exc:
            _loop_log.warning("Door-knock Excel sync loop error: %s", exc)
            _beat("door_knock_excel_sync", "error", str(exc))
        await asyncio.sleep(DOOR_KNOCK_SYNC_POLL_SECONDS)


# ---------- named agent loops ----------

async def _nyla_pipeline_loop():
    """Nyla — CRM pipeline manager. Checks hot leads, stalled deals, overdue follow-ups."""
    await asyncio.sleep(180)
    _loop_log.info("[Nyla] Pipeline manager agent started")
    while True:
        try:
            now = _datetime.now(SYDNEY_TZ)
            if not (7 <= now.hour < 21):
                await asyncio.sleep(1800)
                continue
            _beat("nyla_pipeline", "running")
            from services.agent_pipeline_manager import run_cycle
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await run_cycle(session)
                if result.get("tickets_raised", 0) > 0:
                    _loop_log.info("[Nyla] %s", result)
            _beat("nyla_pipeline", "healthy")
        except Exception as exc:
            _loop_log.warning("[Nyla] cycle error: %s", exc)
            _beat("nyla_pipeline", "error", str(exc))
        await asyncio.sleep(2 * 3600)  # every 2 hours


async def _rex_listing_loop():
    """Rex — REA listing analyst. Tracks performance, flags underperformers, spots variants."""
    await asyncio.sleep(360)
    _loop_log.info("[Rex] Listing analyst agent started")
    while True:
        try:
            now = _datetime.now(SYDNEY_TZ)
            if not (7 <= now.hour < 22):
                await asyncio.sleep(1800)
                continue
            _beat("rex_listing", "running")
            from services.agent_listing_analyst import run_cycle
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await run_cycle(session)
                if result.get("tickets_raised", 0) > 0:
                    _loop_log.info("[Rex] %s", result)
            _beat("rex_listing", "healthy")
        except Exception as exc:
            _loop_log.warning("[Rex] cycle error: %s", exc)
            _beat("rex_listing", "error", str(exc))
        await asyncio.sleep(3 * 3600)  # every 3 hours


async def _reactive_scoring_loop():
    """Boost call_today_score based on live events (tickets, enquiries, overdue)."""
    await asyncio.sleep(240)
    _loop_log.info("[ReactiveScoring] Loop started")
    while True:
        try:
            _beat("reactive_scoring", "running")
            from services.reactive_scoring import run_reactive_scoring
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await run_reactive_scoring(session)
                if result.get("boosted", 0) > 0:
                    _loop_log.info("[ReactiveScoring] %s", result)
            _beat("reactive_scoring", "healthy")
        except Exception as exc:
            _loop_log.warning("[ReactiveScoring] error: %s", exc)
            _beat("reactive_scoring", "error", str(exc))
        await asyncio.sleep(30 * 60)  # every 30 minutes


async def _outreach_sender_loop():
    """Dispatch approved hermes_campaigns (email/SMS)."""
    await asyncio.sleep(300)
    _loop_log.info("[OutreachSender] Loop started")
    while True:
        try:
            _beat("outreach_sender", "running")
            from services.outreach_sender import run_outreach_sender
            session_factory = _get_session_factory()
            async with session_factory() as session:
                result = await run_outreach_sender(session)
                if result.get("sent", 0) > 0:
                    _loop_log.info("[OutreachSender] %s", result)
            _beat("outreach_sender", "healthy")
        except Exception as exc:
            _loop_log.warning("[OutreachSender] error: %s", exc)
            _beat("outreach_sender", "error", str(exc))
        await asyncio.sleep(5 * 60)  # every 5 minutes


# ---------- worker/scheduler wrappers used by job_registry ----------
async def _background_sender_loop():
    from services.automations import _background_sender_loop as _impl
    await _impl()


async def _control_runtime_loop():
    from services.control_service import _control_runtime_loop as _impl
    await _impl()


async def _orchestration_loop():
    from services.orchestration_loop import _orchestration_loop as _impl
    await _impl()


async def _system_health_pulse():
    from services.health_service import _system_health_pulse as _impl
    await _impl()


async def _reaxml_poll_loop():
    from services.reaxml_ingestor import _reaxml_poll_loop as _impl
    await _impl()


async def _sitemap_validation_loop():
    from services.sitemap_ingestor import _sitemap_validation_loop as _impl
    await _impl()


async def _daily_delta_loop():
    from services.delta_engine import _daily_delta_loop as _impl
    await _impl()


async def _self_improvement_loop():
    """
    Self-adapting feedback loop — reviews outcomes, stores learnings, adjusts priorities.

    Runs every 2 hours during business hours:
    1. Reviews call outcomes from the last 24h → stores learnings
    2. Identifies which call angles/signals led to positive outcomes
    3. Detects stalled leads needing re-engagement
    4. Reviews REA listing performance → stores optimization findings
    5. Creates follow-up tickets for leads that need attention
    """
    await asyncio.sleep(600)  # wait 10 min after boot
    _loop_log.info("[SelfImprove] Self-improvement feedback loop started")
    while True:
        try:
            now = _datetime.now(SYDNEY_TZ)
            if not (7 <= now.hour < 21):
                await asyncio.sleep(1800)
                continue

            _beat("self_improve", "running")
            session_factory = _get_session_factory()
            async with session_factory() as session:
                await _run_self_improvement_cycle(session)
            _beat("self_improve", "healthy")
        except Exception as exc:
            _loop_log.warning("[SelfImprove] cycle error: %s", exc)
            _beat("self_improve", "error", str(exc))
        await asyncio.sleep(2 * 3600)  # every 2 hours


async def _run_self_improvement_cycle(session) -> None:
    """Execute one self-improvement cycle."""
    import json
    import hashlib
    from sqlalchemy import text
    from core.utils import now_iso
    from core.config import USE_POSTGRES

    now = now_iso()

    # Dialect helpers — scheduler runs on Postgres in production, SQLite locally
    _24h_ago = "NOW() - INTERVAL '24 hours'" if USE_POSTGRES else "datetime('now', '-24 hours')"
    _30d_ago = "NOW() - INTERVAL '30 days'" if USE_POSTGRES else "datetime('now', '-30 days')"
    _14d_ago = "NOW() - INTERVAL '14 days'" if USE_POSTGRES else "datetime('now', '-14 days')"
    _7d_ago = "NOW() - INTERVAL '7 days'" if USE_POSTGRES else "datetime('now', '-7 days')"

    def _upsert_finding(cols: str, vals: str) -> str:
        if USE_POSTGRES:
            return f"INSERT INTO hermes_findings ({cols}) VALUES ({vals}) ON CONFLICT (id) DO NOTHING"
        return f"INSERT OR IGNORE INTO hermes_findings ({cols}) VALUES ({vals})"

    def _upsert_memory() -> str:
        if USE_POSTGRES:
            return (
                "INSERT INTO hermes_memory (id, memory_type, title, body, confidence_score, created_at) "
                "VALUES (:id, 'pattern', :title, :body, 0.85, :now) "
                "ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, created_at = EXCLUDED.created_at"
            )
        return (
            "INSERT OR REPLACE INTO hermes_memory "
            "(id, memory_type, title, body, confidence_score, created_at) "
            "VALUES (:id, 'pattern', :title, :body, 0.85, :now)"
        )

    _finding_cols = (
        "id, source_id, source_type, source_name, source_url, dedupe_key, "
        "company_scope, topic, signal_type, summary, why_it_matters, "
        "confidence_score, actionability_score, novelty_score, created_at"
    )

    # ── 1. Review call outcomes from last 24h ──
    try:
        call_rows = (await session.execute(text(f"""
            SELECT outcome, COUNT(*) as cnt,
                   AVG(call_duration_seconds) as avg_duration
            FROM call_log
            WHERE logged_at >= {_24h_ago}
            GROUP BY outcome ORDER BY cnt DESC
        """))).mappings().all()

        if call_rows:
            total_calls = sum(r["cnt"] for r in call_rows)
            connected = sum(r["cnt"] for r in call_rows if "connected" in (r["outcome"] or ""))
            booked = sum(r["cnt"] for r in call_rows if "booked" in (r["outcome"] or ""))
            connect_rate = round(connected / max(total_calls, 1) * 100, 1)
            book_rate = round(booked / max(total_calls, 1) * 100, 1)

            breakdown = ", ".join(
                str(r["outcome"]) + "=" + str(r["cnt"]) for r in call_rows[:6]
            )
            summary = (
                f"24h call outcomes: {total_calls} calls, "
                f"{connect_rate}% connect rate, {book_rate}% booking rate. "
                f"Breakdown: {breakdown}"
            )

            dedupe_key = hashlib.md5(f"self_improve:calls:{now[:13]}".encode()).hexdigest()
            finding_id = hashlib.md5(f"self_improve:calls:{now}".encode()).hexdigest()
            await session.execute(text(_upsert_finding(
                _finding_cols,
                ":id, 'self_improve', 'feedback_loop', 'Self-Improvement Engine', '', "
                ":dk, 'shared', :topic, 'call_performance', :summary, "
                "'Tracks calling effectiveness to optimize future call lists', "
                "0.9, 0.85, 0.6, :now"
            )), {
                "id": finding_id, "dk": dedupe_key,
                "topic": f"Call Performance: {connect_rate}% connect, {book_rate}% book",
                "summary": summary, "now": now,
            })
            _loop_log.info("[SelfImprove] Call outcomes: %s", summary[:120])
    except Exception as exc:
        _loop_log.debug("[SelfImprove] Call outcome review failed: %s", exc)

    # ── 2. Identify winning signal types (which signals lead to booked appraisals) ──
    try:
        signal_rows = (await session.execute(text(f"""
            SELECT l.signal_status, COUNT(*) as cnt,
                   SUM(CASE WHEN l.status IN ('appt_booked','mortgage_appt_booked','converted')
                       THEN 1 ELSE 0 END) as wins
            FROM leads l
            WHERE l.last_contacted_at >= {_30d_ago}
              AND l.signal_status IS NOT NULL
            GROUP BY l.signal_status
            HAVING COUNT(*) >= 3
            ORDER BY (CAST(SUM(CASE WHEN l.status IN ('appt_booked','mortgage_appt_booked','converted') THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*)) DESC
        """))).mappings().all()

        if signal_rows:
            best_signals = [
                f"{r['signal_status']}={r['wins']}/{r['cnt']}"
                for r in signal_rows[:5]
            ]
            signal_summary = f"30d signal effectiveness: {', '.join(best_signals)}"

            dedupe_key = hashlib.md5(f"self_improve:signals:{now[:10]}".encode()).hexdigest()
            finding_id = hashlib.md5(f"self_improve:signals:{now}".encode()).hexdigest()
            await session.execute(text(_upsert_finding(
                _finding_cols,
                ":id, 'self_improve', 'feedback_loop', 'Self-Improvement Engine', '', "
                ":dk, 'shared', :topic, 'signal_effectiveness', :summary, "
                "'Identifies which lead signals convert best to inform call prioritization', "
                "0.85, 0.9, 0.5, :now"
            )), {
                "id": finding_id, "dk": dedupe_key,
                "topic": "Signal Effectiveness Report",
                "summary": signal_summary, "now": now,
            })
            _loop_log.info("[SelfImprove] Signal effectiveness: %s", signal_summary[:120])
    except Exception as exc:
        _loop_log.debug("[SelfImprove] Signal effectiveness review failed: %s", exc)

    # ── 3. Stalled leads — now handled by Nyla (agent_pipeline_manager) ──

    # ── 4. REA listing performance review ──
    try:
        rea_rows = (await session.execute(text("""
            SELECT id, address, suburb, rea_views, rea_enquiries,
                   rea_title_variant, rea_desc_variant, rea_last_edit_at
            FROM leads
            WHERE COALESCE(rea_listing_id, '') <> ''
              AND (LOWER(COALESCE(property_type, '')) = 'land'
                   OR LOWER(COALESCE(trigger_type, '')) = 'bathla_land')
        """))).mappings().all()

        if rea_rows:
            total_views = sum(int(r.get("rea_views") or 0) for r in rea_rows)
            total_enq = sum(int(r.get("rea_enquiries") or 0) for r in rea_rows)
            avg_ctr = round(total_enq / max(total_views, 1) * 100, 2)

            # Find best/worst performing title variants
            variant_perf: Dict[str, Dict[str, Any]] = {}
            for r in rea_rows:
                v = r.get("rea_title_variant") or "default"
                if v not in variant_perf:
                    variant_perf[v] = {"views": 0, "enq": 0, "count": 0}
                variant_perf[v]["views"] += int(r.get("rea_views") or 0)
                variant_perf[v]["enq"] += int(r.get("rea_enquiries") or 0)
                variant_perf[v]["count"] += 1

            variant_summary = ", ".join(
                f"{k}: {p['enq']}/{p['views']} ({round(p['enq']/max(p['views'],1)*100,1)}% CTR)"
                for k, p in sorted(variant_perf.items(), key=lambda x: x[1]["enq"], reverse=True)[:4]
            )

            rea_summary = (
                f"REA portfolio: {len(rea_rows)} live, {total_views} views, "
                f"{total_enq} enquiries, {avg_ctr}% CTR. Variants: {variant_summary}"
            )

            dedupe_key = hashlib.md5(f"self_improve:rea:{now[:13]}".encode()).hexdigest()
            finding_id = hashlib.md5(f"self_improve:rea:{now}".encode()).hexdigest()
            await session.execute(text(_upsert_finding(
                _finding_cols,
                ":id, 'self_improve', 'feedback_loop', 'Self-Improvement Engine', '', "
                ":dk, 'shared', :topic, 'rea_performance', :summary, "
                "'Tracks REA listing performance to optimize copy and targeting', "
                "0.9, 0.9, 0.65, :now"
            )), {
                "id": finding_id, "dk": dedupe_key,
                "topic": f"REA Portfolio: {avg_ctr}% CTR across {len(rea_rows)} listings",
                "summary": rea_summary, "now": now,
            })
            _loop_log.info("[SelfImprove] REA performance: %s", rea_summary[:120])
    except Exception as exc:
        _loop_log.debug("[SelfImprove] REA performance review failed: %s", exc)

    # ── 5. Store adaptive learning in hermes_memory ──
    try:
        # Count how many times each call outcome led to a next action
        outcome_rows = (await session.execute(text(f"""
            SELECT l.last_outcome, l.status,
                   COUNT(*) as cnt
            FROM leads l
            WHERE l.last_outcome IS NOT NULL
              AND l.last_contacted_at >= {_14d_ago}
            GROUP BY l.last_outcome, l.status
            ORDER BY cnt DESC LIMIT 20
        """))).mappings().all()

        if outcome_rows:
            learning = "Outcome→Status patterns (14d): " + "; ".join(
                f"{r['last_outcome']}→{r['status']}({r['cnt']})"
                for r in outcome_rows[:10]
            )

            mem_id = hashlib.md5(f"learning:outcomes:{now[:10]}".encode()).hexdigest()
            await session.execute(text(_upsert_memory()), {
                "id": mem_id,
                "title": "Call Outcome Patterns",
                "body": learning,
                "now": now,
            })
            _loop_log.info("[SelfImprove] Stored learning: %s", learning[:100])
    except Exception as exc:
        _loop_log.debug("[SelfImprove] Learning storage failed: %s", exc)

    await session.commit()
