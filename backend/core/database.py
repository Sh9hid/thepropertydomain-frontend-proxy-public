import datetime
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
import smtplib
import sqlite3
from base64 import b64encode
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, AsyncGenerator
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlencode, urlparse

import httpx
import redis.asyncio as redis
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from sqlalchemy import text, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, select

import core.config
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from models.sql_models import Lead, Task, Appointment, SoldEvent, CommunicationAccount, Agent, LeadNote, CallLog, TickerEvent, LeadInteraction  # noqa: F401
from models.intelligence_models import IntelligenceAgentProfile, IntelligenceEvent, IntelligenceLeadIntelligence, IntelligenceMedia, IntelligenceParty, IntelligenceProperty, IntelligencePropertyParty  # noqa: F401
from models.sales_core_models import BusinessContext, ContactAttempt, ContentAsset, EnrichmentState, LeadContact, LeadState, ProviderUsageLog, TaskQueue  # noqa: F401
from core.logic import ensure_project_memory_file

# --- Modern Async PostgreSQL / SQLModel Support ---
_LOCAL_DB_HOSTS = {"", "localhost", "127.0.0.1", "0.0.0.0", "::1", "db", "postgres"}


def _is_local_database_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").strip().lower()
    return host in _LOCAL_DB_HOSTS


def _fix_asyncpg_url(url: str) -> str:
    """Normalize Postgres URLs for async SQLAlchemy engine with asyncpg.

    Neon provides URLs with ?sslmode=require which asyncpg doesn't understand.
    asyncpg uses ?ssl=require instead. Also strip channel_binding which asyncpg
    doesn't support. For managed Postgres hosts, default to SSL if unset.
    Also upgrades plain/psycopg DSNs to asyncpg DSNs for create_async_engine.
    """
    if not url or url.startswith("sqlite"):
        return url

    normalized = url.strip()
    if normalized.startswith("postgres://"):
        normalized = normalized.replace("postgres://", "postgresql+asyncpg://", 1)
    elif normalized.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif normalized.startswith("postgresql+psycopg2://"):
        normalized = normalized.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    elif normalized.startswith("postgresql+psycopg://"):
        normalized = normalized.replace("postgresql+psycopg://", "postgresql+asyncpg://", 1)

    parsed = urlparse(normalized)
    params = dict(urllib_parse.parse_qsl(parsed.query))
    changed = False
    if "sslmode" in params:
        params["ssl"] = params.pop("sslmode")
        changed = True
    if "channel_binding" in params:
        params.pop("channel_binding")
        changed = True
    if "ssl" not in params and not _is_local_database_url(url):
        params["ssl"] = "require"
        changed = True
    if not changed and normalized == url:
        return url
    new_query = urlencode(params)
    return parsed._replace(query=new_query).geturl()


_engine_kwargs: dict = {"echo": False, "future": True, "pool_pre_ping": True}
if not core.config.DATABASE_URL.startswith("sqlite"):
    _engine_kwargs["pool_size"] = 5
    _engine_kwargs["max_overflow"] = 5
    _engine_kwargs["pool_recycle"] = 300
    _engine_kwargs["pool_timeout"] = 30
    _engine_kwargs["connect_args"] = {
        "command_timeout": 30,
        "server_settings": {
            "application_name": "woonona-backend",
        },
    }
    if not _is_local_database_url(core.config.DATABASE_URL):
        _engine_kwargs["connect_args"]["ssl"] = "require"

async_engine = create_async_engine(_fix_asyncpg_url(core.config.DATABASE_URL), **_engine_kwargs)

# --- SQLite ATTACH logic for intelligence schema compatibility ---
@event.listens_for(async_engine.sync_engine, "connect")
def set_sqlite_attach(dbapi_connection, connection_record):
    if core.config.DATABASE_URL.startswith("sqlite"):
        cursor = dbapi_connection.cursor()
        try:
            # This allows raw SQL like 'intelligence.event' to work by mapping 
            # the 'intelligence' schema to the main leads.db file.
            cursor.execute(f"ATTACH DATABASE '{core.config.DB_PATH}' AS intelligence")
        except Exception:
            pass # already attached or path issue
        cursor.close()

# Single factory instance — creating sessionmaker per-request is wasteful
_async_session_factory = sessionmaker(
    async_engine, class_=AsyncSession, expire_on_commit=False
)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with _async_session_factory() as session:
        yield session


REVENUE_ENGINE_LEAD_COLUMNS: Dict[str, str] = {
    "last_contacted_at": "TEXT",
    "follow_up_due_at": "TEXT",
    "status": "TEXT DEFAULT 'captured'",
    "price_drop_count": "INTEGER DEFAULT 0",
    "last_activity_type": "TEXT DEFAULT ''",
    "signal_status": "TEXT DEFAULT ''",
}

REVENUE_ENGINE_CALL_LOG_COLUMNS: Dict[str, str] = {
    "lead_id": "TEXT DEFAULT ''",
    "user_id": "TEXT DEFAULT 'Shahid'",
    "connected": "INTEGER DEFAULT 0",
    "outcome": "TEXT DEFAULT 'unknown'",
    "timestamp": "TEXT DEFAULT ''",
    "call_duration_seconds": "INTEGER DEFAULT 0",
    "transcript": "TEXT",
    "summary": "TEXT",
    "intent_signal": "REAL DEFAULT 0",
    "booking_attempted": "INTEGER DEFAULT 0",
    "objection_tags": "TEXT DEFAULT '[]'",
    "next_step_detected": "INTEGER DEFAULT 0",
}

REVENUE_ENGINE_INDEX_DDLS: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_leads_follow_up_due_at ON leads(follow_up_due_at)",
    "CREATE INDEX IF NOT EXISTS idx_leads_last_contacted_at ON leads(last_contacted_at)",
    "CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_signal_status ON leads(signal_status)",
    "CREATE INDEX IF NOT EXISTS idx_leads_status_created_at ON leads(status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_leads_status_follow_up_due_at ON leads(status, follow_up_due_at)",
    "CREATE INDEX IF NOT EXISTS idx_leads_call_queue ON leads(status, call_today_score DESC, heat_score DESC, updated_at DESC, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_lead ON call_log(lead_id, logged_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_lead_timestamp ON call_log(lead_id, timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_user ON call_log(user_id, logged_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_logged_date_logged_at ON call_log(logged_date, logged_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_operator_logged_at ON call_log(operator, logged_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_call_log_timestamp ON call_log(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_call_recording_status ON call_log(recording_status)",
    "CREATE INDEX IF NOT EXISTS idx_tasks_followup_due ON tasks(task_type, status, due_at)",
    "CREATE INDEX IF NOT EXISTS idx_lead_interactions_lead_created ON lead_interactions(lead_id, created_at DESC)",
)

SALES_CORE_CONTACT_ATTEMPT_COLUMNS: Dict[str, str] = {
    "recipient_email": "TEXT",
    "email_subject": "TEXT",
    "email_body": "TEXT",
    "sequence_key": "TEXT",
    "sequence_step": "INTEGER",
    "variant_key": "TEXT",
    "parent_attempt_id": "TEXT",
    "external_message_id": "TEXT",
    "opened_at": "TEXT",
    "replied_at": "TEXT",
    "performance_json": "TEXT DEFAULT '{}'",
}

WAITLIST_EXTRA_COLUMNS: Dict[str, str] = {
    "suburb_interest": "TEXT",
    "offer_code": "TEXT DEFAULT '3_guides_bundle'",
}


async def _sqlite_table_columns(conn, table_name: str) -> set[str]:
    if core.config.DATABASE_URL.startswith("sqlite"):
        rows = await conn.execute(text(f"PRAGMA table_info({table_name})"))
        return {str(row[1]) for row in rows.fetchall()}
    else:
        # PostgreSQL equivalent
        rows = await conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = :table",
        ), {"table": table_name})
        return {str(row[0]) for row in rows.fetchall()}


async def _ensure_sqlite_columns(conn, table_name: str, columns: Dict[str, str]) -> None:
    existing = await _sqlite_table_columns(conn, table_name)
    if not existing:
        return
    for column_name, column_def in columns.items():
        if column_name in existing:
            continue
        await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}"))
        existing.add(column_name)


async def ensure_revenue_engine_schema(conn) -> None:
    await _ensure_sqlite_columns(conn, "leads", REVENUE_ENGINE_LEAD_COLUMNS)
    await _ensure_sqlite_columns(conn, "call_log", REVENUE_ENGINE_CALL_LOG_COLUMNS)
    await _ensure_sqlite_columns(conn, "contact_attempt", SALES_CORE_CONTACT_ATTEMPT_COLUMNS)
    await _ensure_sqlite_columns(conn, "propella_waitlist", WAITLIST_EXTRA_COLUMNS)
    await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS lead_audit_log (
            id TEXT PRIMARY KEY,
            lead_id TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT 'lead',
            action TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '',
            actor TEXT NOT NULL DEFAULT 'system',
            batch_id TEXT DEFAULT '',
            before_state TEXT NOT NULL DEFAULT '{}',
            after_state TEXT NOT NULL DEFAULT '{}',
            payload TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
    """))
    await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lead_audit_log_lead_created ON lead_audit_log(lead_id, created_at DESC)"))
    await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lead_audit_log_batch ON lead_audit_log(batch_id, created_at DESC)"))
    for ddl in REVENUE_ENGINE_INDEX_DDLS:
        await conn.execute(text(ddl))

# --- Redis Support ---
redis_client: Optional[redis.Redis] = None


class _MemoryRedis:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._expiry: dict[str, float] = {}

    def _prune(self) -> None:
        if not self._expiry:
            return
        now = asyncio.get_running_loop().time()
        expired = [key for key, deadline in self._expiry.items() if deadline <= now]
        for key in expired:
            self._values.pop(key, None)
            self._expiry.pop(key, None)

    async def ping(self) -> bool:
        self._prune()
        return True

    async def get(self, key: str) -> Optional[str]:
        self._prune()
        return self._values.get(key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        self._prune()
        self._values[key] = str(value)
        if ex is not None:
            self._expiry[key] = asyncio.get_running_loop().time() + max(int(ex), 0)
        else:
            self._expiry.pop(key, None)
        return True

    async def expire(self, key: str, seconds: int) -> bool:
        self._prune()
        if key not in self._values:
            return False
        self._expiry[key] = asyncio.get_running_loop().time() + max(int(seconds), 0)
        return True

    async def delete(self, *keys: str) -> int:
        self._prune()
        deleted = 0
        for key in keys:
            existed = key in self._values
            self._values.pop(key, None)
            self._expiry.pop(key, None)
            deleted += int(existed)
        return deleted


def _should_use_memory_redis() -> bool:
    raw_url = (core.config.REDIS_URL or "").strip()
    if raw_url.lower().startswith("memory://"):
        return True
    host = (urlparse(raw_url).hostname or "").strip().lower()
    if host in {"", "localhost", "127.0.0.1", "::1"}:
        return core.config.APP_ENV == "development"
    if "placeholder" in host or "no-redis" in host:
        return True
    return False


async def get_redis() -> redis.Redis | _MemoryRedis:
    global redis_client
    if redis_client is None:
        if _should_use_memory_redis():
            redis_client = _MemoryRedis()
        else:
            redis_client = redis.from_url(core.config.REDIS_URL, decode_responses=True)
    return redis_client

NEW_LEADS_COLUMNS = [
    ("domain_listing_id", "TEXT"),
    ("domain_enriched_date", "TEXT"),
    ("days_on_market", "INTEGER DEFAULT 0"),
    ("signal_status", "TEXT DEFAULT ''"),
    ("listing_headline", "TEXT"),
    ("h3index", "TEXT"),
    ("rea_listing_id", "TEXT"),
    ("rea_upload_id", "TEXT"),
    ("rea_upload_status", "TEXT"),
    ("rea_last_upload_response", "TEXT"),
    ("rea_last_upload_report", "TEXT"),
    ("last_called_date", "TEXT"),
    ("relisted", "INTEGER DEFAULT 0"),
    ("list_date", "TEXT"),
    ("date_of_birth",    "TEXT"),
    ("id4me_enriched",   "INTEGER DEFAULT 0"),
    ("id4me_enriched_at","TEXT"),
    ("id4me_last_seen",  "TEXT"),
    ("canonical_address", "TEXT"),
    ("address_unit", "TEXT"),
    ("street_number", "TEXT"),
    ("street_name", "TEXT"),
    ("street_type", "TEXT"),
    ("state", "TEXT"),
    ("country_code", "TEXT"),
    ("mailing_address", "TEXT"),
    ("mailing_address_matches_property", "INTEGER DEFAULT 1"),
    ("absentee_owner", "INTEGER DEFAULT 0"),
    ("likely_landlord", "INTEGER DEFAULT 0"),
    ("likely_owner_occupier", "INTEGER DEFAULT 0"),
    ("owner_occupancy_status", "TEXT"),
    ("owner_first_name", "TEXT"),
    ("owner_last_name", "TEXT"),
    ("owner_persona", "TEXT"),
    ("alternate_phones", "TEXT DEFAULT '[]'"),
    ("alternate_emails", "TEXT DEFAULT '[]'"),
    ("phone_status", "TEXT"),
    ("phone_line_type", "TEXT"),
    ("email_status", "TEXT"),
    ("do_not_call", "INTEGER DEFAULT 0"),
    ("consent_status", "TEXT"),
    ("contactability_tier", "TEXT"),
    ("contactability_reasons", "TEXT DEFAULT '[]'"),
    ("property_type", "TEXT"),
    ("parcel_lot", "TEXT"),
    ("parcel_plan", "TEXT"),
    ("title_reference", "TEXT"),
    ("ownership_duration_years", "REAL"),
    ("tenure_bucket", "TEXT"),
    ("owner_type", "TEXT"),
    ("estimated_value_low", "INTEGER"),
    ("estimated_value_mid", "INTEGER"),
    ("estimated_value_high", "INTEGER"),
    ("valuation_confidence", "TEXT"),
    ("valuation_date", "TEXT"),
    ("rental_estimate_low", "INTEGER"),
    ("rental_estimate_high", "INTEGER"),
    ("yield_estimate", "REAL"),
    ("last_listing_status", "TEXT"),
    ("last_listing_date", "TEXT"),
    ("sale_history", "TEXT DEFAULT '[]'"),
    ("listing_status_history", "TEXT DEFAULT '[]'"),
    ("nearby_sales", "TEXT DEFAULT '[]'"),
    ("deterministic_tags", "TEXT DEFAULT '[]'"),
    ("seller_intent_signals", "TEXT DEFAULT '[]'"),
    ("refinance_signals", "TEXT DEFAULT '[]'"),
    ("ownership_notes", "TEXT"),
    ("source_provenance", "TEXT DEFAULT '[]'"),
    ("enrichment_status", "TEXT"),
    ("enrichment_last_synced_at", "TEXT"),
    ("research_status", "TEXT"),
    ("preferred_contact_method", "TEXT DEFAULT ''"),
    ("followup_frequency", "TEXT DEFAULT 'none'"),
    ("market_updates_opt_in", "INTEGER DEFAULT 0"),
    ("next_followup_at", "TEXT"),
    ("followup_status", "TEXT DEFAULT 'active'"),
    ("followup_notes", "TEXT"),
    # REA listing agent columns
    ("rea_title_variant", "INTEGER"),
    ("rea_desc_variant", "INTEGER"),
    ("rea_last_edit_at", "TEXT"),
    ("rea_views", "INTEGER DEFAULT 0"),
    ("rea_enquiries", "INTEGER DEFAULT 0"),
    ("lot_number", "TEXT"),
    ("lot_type", "TEXT"),
    ("frontage", "TEXT"),
    ("project_name", "TEXT"),
    ("listing_description", "TEXT"),
]

TASK_EXTRA_COLUMNS = [
    ("payload_json", "TEXT DEFAULT '{}'"),
    ("attempt_count", "INTEGER DEFAULT 0"),
    ("last_error", "TEXT"),
]

# --- Database Initialization ---
async def init_postgres():
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        if not core.config.DATABASE_URL.startswith("sqlite"):
            try:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            except Exception:
                pass
            try:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            except Exception:
                pass
            try:
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS intelligence;"))
            except Exception:
                pass
            try:
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS org;"))
            except Exception:
                pass
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ingest_runs (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                stats TEXT,
                created_at TEXT NOT NULL
            )
        """))
        
        # Move all listing_tables here to ensure they are created in Postgres too
        listing_tables = [
            """
            CREATE TABLE IF NOT EXISTS source_health (
                source_key TEXT PRIMARY KEY,
                source_type TEXT DEFAULT '',
                source_name TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                last_error_code TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                blocked_until TEXT,
                last_checked_at TEXT,
                last_success_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS email_accounts (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                smtp_host TEXT NOT NULL DEFAULT 'smtp-mail.outlook.com',
                smtp_port INTEGER NOT NULL DEFAULT 587,
                smtp_username TEXT NOT NULL DEFAULT '',
                smtp_password TEXT NOT NULL DEFAULT '',
                from_name TEXT DEFAULT '',
                from_email TEXT DEFAULT '',
                use_tls INTEGER NOT NULL DEFAULT 1,
                daily_cap INTEGER NOT NULL DEFAULT 80,
                is_warmup_mode INTEGER NOT NULL DEFAULT 0,
                warmup_day INTEGER NOT NULL DEFAULT 0,
                sends_today INTEGER NOT NULL DEFAULT 0,
                sends_today_date TEXT DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS propella_waitlist (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                suburb_interest TEXT,
                offer_code TEXT DEFAULT '3_guides_bundle',
                phone TEXT,
                agency TEXT,
                team_size TEXT,
                message TEXT,
                submitted_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS enrichment_jobs (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'cotality',
                status TEXT NOT NULL DEFAULT 'queued',
                requested_fields_json TEXT NOT NULL DEFAULT '[]',
                matched_address TEXT,
                machine_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                created_at TEXT,
                updated_at TEXT,
                completed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS enrichment_results (
                id TEXT PRIMARY KEY,
                enrichment_job_id TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'cotality',
                raw_payload_json TEXT NOT NULL DEFAULT '{}',
                proposed_updates_json TEXT NOT NULL DEFAULT '{}',
                screenshot_path TEXT,
                confidence REAL,
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS run_artifacts (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                artifact_type TEXT DEFAULT 'note',
                title TEXT NOT NULL,
                status TEXT DEFAULT 'ready',
                content TEXT DEFAULT '',
                attributes TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rea_api_logs (
                id TEXT PRIMARY KEY,
                lead_id TEXT,
                rea_upload_id TEXT,
                rea_listing_id TEXT,
                action TEXT NOT NULL,
                request_method TEXT,
                request_path TEXT,
                request_payload TEXT,
                response_status_code INTEGER,
                response_body TEXT,
                ok INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """
        ]
        for ddl in listing_tables:
            await conn.execute(text(ddl))

        # Cotality tables (missing DDL — referenced in routes/services but never created)
        cotality_tables = [
            """
            CREATE TABLE IF NOT EXISTS cotality_reports (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                report_type TEXT NOT NULL DEFAULT 'property_intelligence',
                title TEXT,
                html_content TEXT,
                json_payload TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cotality_accounts (
                id TEXT PRIMARY KEY,
                api_base TEXT,
                api_key TEXT,
                endpoint_path TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_cotality_reports_lead ON cotality_reports(lead_id)
            """
        ]
        for ddl in cotality_tables:
            await conn.execute(text(ddl))

        # REA A/B testing table
        ab_test_tables = [
            """
            CREATE TABLE IF NOT EXISTS rea_ab_tests (
                id TEXT PRIMARY KEY,
                listing_id TEXT NOT NULL,
                address TEXT NOT NULL DEFAULT '',
                test_type TEXT NOT NULL DEFAULT 'headline',
                variant_a TEXT NOT NULL DEFAULT '',
                variant_b TEXT NOT NULL DEFAULT '',
                views_a INTEGER DEFAULT 0,
                views_b INTEGER DEFAULT 0,
                enquiries_a INTEGER DEFAULT 0,
                enquiries_b INTEGER DEFAULT 0,
                ctr_a REAL DEFAULT 0.0,
                ctr_b REAL DEFAULT 0.0,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TEXT NOT NULL,
                winner TEXT,
                confidence REAL,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rea_ab_tests_listing ON rea_ab_tests(listing_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_rea_ab_tests_status ON rea_ab_tests(status)
            """
        ]
        for ddl in ab_test_tables:
            await conn.execute(text(ddl))

        # REA listing templates
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS rea_listing_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'land',
                headline_pattern TEXT NOT NULL,
                body_pattern TEXT NOT NULL,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # Seed default templates if table is empty
        _tpl_count = (await conn.execute(text("SELECT COUNT(*) FROM rea_listing_templates"))).scalar_one()
        if _tpl_count == 0:
            is_sqlite_tpl = core.config.DATABASE_URL.startswith("sqlite")
            _DISCLAIMER = (
                "Disclaimer: The above information has been gathered from sources that we believe are reliable."
                " However, Laing+Simmons real estate agency cannot guarantee the accuracy of this information"
                " and nor do we accept any responsibility for its accuracy. Any interested parties should rely"
                " on their own enquiries and judgment to determine the accuracy of this information for their own purposes."
            )
            _seed_tpls = [
                {
                    "id": "tpl_first_home",
                    "name": "First Home Builder",
                    "category": "land",
                    "headline_pattern": "Build-Ready {land_size}sqm in {suburb}",
                    "body_pattern": (
                        "{principal_name} from {brand_name} is pleased to present Lot {lot_number}\n"
                        "\u2014 a {land_size} sqm registered block {position_phrase}.\n"
                        "{lot_type_line}\n\n"
                        "If you've been searching for the right block to build your first home, this one ticks every box.\n\n"
                        "Land size: {land_size} sqm\n"
                        "Frontage: {frontage}\n"
                        "{registration_line}\n\n"
                        "{location_block}\n\n"
                        "Choose your own builder or work with us to arrange a quality custom build\n"
                        "at a competitive price with build time guarantee.\n\n"
                        "To discuss this opportunity, contact {principal_name} on {principal_phone}\n"
                        "or email {principal_email}.\n\n"
                        "{disclaimer}"
                    ),
                },
                {
                    "id": "tpl_investor",
                    "name": "Investor Yield",
                    "category": "land",
                    "headline_pattern": "Titled {land_size}sqm Land \u2014 Lot {lot_number}, {suburb} | ${price}",
                    "body_pattern": (
                        "Nitin Puri from Laing+Simmons Oakville | Windsor presents "
                        "Lot {lot_number} \u2014 {land_size} sqm of titled land in {suburb}, one of "
                        "North West Sydney\u2019s highest-demand growth corridors. {lot_type_line}\n\n"
                        "New housing demand in {suburb} consistently outpaces supply. "
                        "Infrastructure spend is accelerating across the North West, and entry "
                        "pricing at ${price} sits well below the suburb\u2019s median house price. "
                        "Whether you\u2019re building to rent, holding for capital growth, or "
                        "constructing for owner-occupier resale, the fundamentals here are sound.\n\n"
                        "Key details:\n"
                        "\u2022 Land size: {land_size} sqm\n"
                        "\u2022 Frontage: {frontage}\n"
                        "\u2022 {registration_line}\n"
                        "\u2022 Price: ${price}\n\n"
                        "{location_block}\n\n"
                        "Multiple blocks available across this release. Contact us for the full "
                        "availability schedule and pricing across all lot sizes.\n\n"
                        "For enquiries, contact Nitin Puri on 0430 042 041 or email "
                        "oakville@lsre.com.au.\n\n"
                        + _DISCLAIMER
                    ),
                },
                {
                    "id": "tpl_family",
                    "name": "Family Upgrader",
                    "category": "land",
                    "headline_pattern": "{land_size}sqm Family Block in {suburb} \u2014 Lot {lot_number} | ${price}",
                    "body_pattern": (
                        "Nitin Puri from Laing+Simmons Oakville | Windsor is delighted to present "
                        "Lot {lot_number} \u2014 {land_size} sqm in {suburb}, sized for the family "
                        "that needs room to grow. {lot_type_line}\n\n"
                        "{frontage} of street frontage. {land_size} square metres to work with. "
                        "Enough space for a proper backyard, a double garage, a home office, and "
                        "bedrooms the kids won\u2019t outgrow in two years. No strata. No shared "
                        "walls. Your land, your architect, your rules.\n\n"
                        "Key details:\n"
                        "\u2022 Land size: {land_size} sqm\n"
                        "\u2022 Frontage: {frontage}\n"
                        "\u2022 {registration_line}\n"
                        "\u2022 Price: ${price}\n\n"
                        "{location_block}\n\n"
                        "Build with your own team or work with us to arrange a quality custom "
                        "build at a competitive price with build time guarantee.\n\n"
                        "To book a site visit or discuss your options, contact Nitin Puri "
                        "on 0430 042 041 or email oakville@lsre.com.au.\n\n"
                        + _DISCLAIMER
                    ),
                },
            ]
            for tpl in _seed_tpls:
                if is_sqlite_tpl:
                    await conn.execute(text(
                        "INSERT OR IGNORE INTO rea_listing_templates (id, name, category, headline_pattern, body_pattern, is_default)"
                        " VALUES (:id, :name, :category, :headline_pattern, :body_pattern, 1)"
                    ), tpl)
                else:
                    await conn.execute(text(
                        "INSERT INTO rea_listing_templates (id, name, category, headline_pattern, body_pattern, is_default)"
                        " VALUES (:id, :name, :category, :headline_pattern, :body_pattern, true)"
                        " ON CONFLICT(id) DO NOTHING"
                    ), tpl)

        # This will create tables if they don't exist based on SQLModel metadata
        await conn.run_sync(SQLModel.metadata.create_all)
        await ensure_sales_core_schema(conn)
        
        # Add missing columns if they don't exist for postgres
        for col_name, col_type in [("contacts", "JSONB DEFAULT '[]'::jsonb"), ("potential_contacts", "JSONB DEFAULT '[]'::jsonb")]:
            try:
                await conn.execute(text(f"ALTER TABLE leads ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            except Exception:
                pass
        for col_name, col_type in NEW_LEADS_COLUMNS:
            try:
                await conn.execute(text(f"ALTER TABLE leads ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            except Exception:
                pass
        for col_name, col_type in TASK_EXTRA_COLUMNS:
            try:
                await conn.execute(text(f"ALTER TABLE tasks ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
            except Exception:
                pass
        # JSONB columns are Postgres-native; SQLite stores them as TEXT.
        is_sqlite_dialect = core.config.DATABASE_URL.startswith("sqlite")
        json_column_type = "TEXT DEFAULT '{}'" if is_sqlite_dialect else "JSONB DEFAULT '{}'::jsonb"
        await conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS lead_interactions (
                    id TEXT PRIMARY KEY,
                    lead_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    direction TEXT,
                    summary TEXT DEFAULT '',
                    payload_json {json_column_type},
                    actor TEXT,
                    source TEXT,
                    created_at TEXT
                )
                """
            )
        )
        await conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS email_delivery_events (
                    id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL DEFAULT 'smtp',
                    account_id TEXT,
                    recipient_email TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message_id TEXT,
                    reason TEXT,
                    payload_json {json_column_type},
                    created_at TEXT
                )
                """
            )
        )
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tasks_followup_due ON tasks(task_type, status, due_at)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lead_interactions_lead_created ON lead_interactions(lead_id, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_email_delivery_events_account_created ON email_delivery_events(account_id, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_email_delivery_events_recipient_created ON email_delivery_events(recipient_email, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sold_events_suburb_sale_date ON sold_events(suburb, sale_date DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transcripts_call_id_created ON transcripts(call_id, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_score_snapshots_entity_lookup ON score_snapshots(entity_type, entity_id, computed_at DESC, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_score_snapshots_call_lookup ON score_snapshots(call_id, computed_at DESC, created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_score_components_snapshot_name ON score_components(snapshot_id, score_name)"))
        # pgvector + pg_trgm indexes are Postgres-only; SQLite gets a minimal
        # search-index table so routes that reference it don't 500.
        if is_sqlite_dialect:
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS lead_search_index (
                        lead_id TEXT PRIMARY KEY,
                        search_text TEXT NOT NULL DEFAULT '',
                        updated_at TEXT
                    )
                    """
                )
            )
        else:
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS lead_search_index (
                        lead_id TEXT PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
                        search_text TEXT NOT NULL DEFAULT '',
                        embedding vector(256),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lead_search_text_trgm ON lead_search_index USING gin (search_text gin_trgm_ops)"))
            try:
                await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_lead_search_embedding ON lead_search_index USING hnsw (embedding vector_cosine_ops)"))
            except Exception:
                pass


async def ensure_sales_core_schema(conn) -> None:
    sales_core_alters = [
        ("contact_attempt", "recipient_email", "TEXT"),
        ("contact_attempt", "email_subject", "TEXT"),
        ("contact_attempt", "email_body", "TEXT"),
        ("contact_attempt", "sequence_key", "TEXT"),
        ("contact_attempt", "sequence_step", "INTEGER"),
        ("contact_attempt", "variant_key", "TEXT"),
        ("contact_attempt", "parent_attempt_id", "TEXT"),
        ("contact_attempt", "external_message_id", "TEXT"),
        ("contact_attempt", "opened_at", "TIMESTAMPTZ"),
        ("contact_attempt", "replied_at", "TIMESTAMPTZ"),
        ("contact_attempt", "performance_json", "JSONB DEFAULT '{}'::jsonb"),
    ]
    for table_name, column_name, column_def in sales_core_alters:
        try:
            await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_def}"))
        except Exception:
            pass

    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_contact_business_phone
            ON lead_contact (business_context_key, primary_phone)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_contact_attempt_lookup
            ON contact_attempt (lead_contact_id, attempted_at DESC)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_contact_attempt_email_variant
            ON contact_attempt (business_context_key, channel, variant_key, sequence_step)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_lead_state_callable_queue
            ON lead_state (business_context_key, callable_now, next_action_due_at, queue_score DESC)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_task_queue_due
            ON task_queue (business_context_key, status, due_at, priority DESC)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_enrichment_state_claim
            ON enrichment_state (business_context_key, source, status, next_retry_at, priority_score DESC)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_provider_usage_feature
            ON provider_usage_log (feature, created_at DESC)
            """
        )
    )
    await conn.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS idx_content_asset_lookup
            ON content_asset (business_context_key, asset_type, created_at DESC)
            """
        )
    )
    timestamp_now = datetime.datetime.now(datetime.timezone.utc)
    await conn.execute(
        text(
            """
            INSERT INTO business_context (key, label, description, active, created_at, updated_at)
            VALUES
                ('real_estate', 'Real Estate', 'Property owner and vendor workflows', true, :timestamp_now, :timestamp_now),
                ('mortgage', 'Mortgage', 'Broker and refinance workflows', true, :timestamp_now, :timestamp_now),
                ('app_saas', 'App / SaaS', 'Software sales workflows', true, :timestamp_now, :timestamp_now)
            ON CONFLICT (key) DO UPDATE
            SET label = EXCLUDED.label,
                description = EXCLUDED.description,
                active = EXCLUDED.active,
                updated_at = :timestamp_now
            """
        ),
        {"timestamp_now": timestamp_now},
    )


def init_db():
    if core.config.DATABASE_URL.startswith("sqlite"):
        asyncio.run(init_sqlite_migrations())
    else:
        asyncio.run(init_postgres())


def _checkpoint_sqlite_wal_best_effort(db_path: str) -> None:
    """
    WAL checkpointing is maintenance, not required for correctness.
    Run it on a fresh sqlite3 connection after migrations commit so startup
    doesn't fail if the driver/connection state rejects the checkpoint.
    """
    try:
        import sqlite3

        with sqlite3.connect(db_path, timeout=1) as conn:
            conn.execute("PRAGMA wal_checkpoint(FULL)")
    except sqlite3.OperationalError as exc:
        print(f"[sqlite] skipping WAL checkpoint for {db_path}: {exc}")


async def init_sqlite_migrations():
    """Add new columns and indexes to SQLite leads table without dropping existing data."""
    import sqlite3

    new_columns = list(NEW_LEADS_COLUMNS)
    new_indexes = [
        ("idx_leads_heat_score",       "leads",                  "heat_score"),
        ("idx_leads_call_today_score", "leads",                  "call_today_score DESC"),
        ("idx_leads_created_at",       "leads",                  "created_at"),
        ("idx_leads_domain_listing",   "leads",                  "domain_listing_id"),
        ("idx_leads_h3index",          "leads",                  "h3index"),
        ("idx_leads_next_action_at",   "leads",                  "next_action_at"),
        ("idx_leads_follow_up_due_at", "leads",                  "follow_up_due_at"),
        ("idx_leads_status",           "leads",                  "status"),
        ("idx_leads_signal_status",    "leads",                  "signal_status"),
        ("idx_leads_queue_bucket",     "leads",                  "queue_bucket"),
        ("idx_leads_canonical_address","leads",                  "canonical_address"),
        ("idx_leads_absentee_owner",   "leads",                  "absentee_owner"),
        ("idx_leads_likely_landlord",  "leads",                  "likely_landlord"),
        ("idx_leads_contactability_tier","leads",                "contactability_tier"),
        ("idx_leads_tenure_bucket",    "leads",                  "tenure_bucket"),
        ("idx_leads_enrichment_status","leads",                  "enrichment_status"),
        ("idx_leads_next_followup_at", "leads",                  "next_followup_at"),
        ("idx_leads_followup_status",  "leads",                  "followup_status"),
        ("idx_comm_accounts_provider", "communication_accounts", "provider"),
        ("idx_leads_rea_listing",      "leads",                  "rea_listing_id"),
        ("idx_tasks_followup_due",     "tasks",                  "task_type, status, due_at"),
        ("idx_lead_interactions_lead_created", "lead_interactions", "lead_id, created_at DESC"),
    ]
    async with async_engine.begin() as conn:
        # SQLite performance PRAGMAs — set once, persist for this connection
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA synchronous=NORMAL"))
        await conn.execute(text("PRAGMA cache_size=-64000"))   # 64 MB page cache
        await conn.execute(text("PRAGMA temp_store=MEMORY"))
        await conn.execute(text("PRAGMA mmap_size=268435456")) # 256 MB mmap
        await conn.run_sync(lambda sync_conn: Lead.__table__.create(sync_conn, checkfirst=True))
        await conn.run_sync(lambda sync_conn: Task.__table__.create(sync_conn, checkfirst=True))
        await conn.run_sync(lambda sync_conn: LeadInteraction.__table__.create(sync_conn, checkfirst=True))

        listing_tables = [
            """
            CREATE TABLE IF NOT EXISTS source_health (
                source_key TEXT PRIMARY KEY,
                source_type TEXT DEFAULT '',
                source_name TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'active',
                last_error_code TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                blocked_until TEXT,
                last_checked_at TEXT,
                last_success_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS email_accounts (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                smtp_host TEXT NOT NULL DEFAULT 'smtp-mail.outlook.com',
                smtp_port INTEGER NOT NULL DEFAULT 587,
                smtp_username TEXT NOT NULL DEFAULT '',
                smtp_password TEXT NOT NULL DEFAULT '',
                from_name TEXT DEFAULT '',
                from_email TEXT DEFAULT '',
                use_tls INTEGER NOT NULL DEFAULT 1,
                daily_cap INTEGER NOT NULL DEFAULT 80,
                is_warmup_mode INTEGER NOT NULL DEFAULT 0,
                warmup_day INTEGER NOT NULL DEFAULT 0,
                sends_today INTEGER NOT NULL DEFAULT 0,
                sends_today_date TEXT DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listing_workflows (
                lead_id TEXT PRIMARY KEY,
                authority_type TEXT DEFAULT 'exclusive',
                stage TEXT DEFAULT 'documents',
                inspection_required INTEGER DEFAULT 1,
                inspection_complete INTEGER DEFAULT 0,
                price_guidance_required INTEGER DEFAULT 1,
                price_guidance_status TEXT DEFAULT 'draft_missing',
                authority_pack_status TEXT DEFAULT 'draft_missing',
                market_ready INTEGER DEFAULT 0,
                lawyer_signoff_status TEXT DEFAULT 'pending',
                marketing_payment_status TEXT DEFAULT 'not_requested',
                workflow_notes TEXT,
                inspection_report_id TEXT,
                approved_price_guidance_id TEXT,
                latest_signing_session_id TEXT,
                pack_document_id TEXT,
                pack_sent_at TEXT,
                pack_signed_at TEXT,
                market_ready_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS listing_documents (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                label TEXT NOT NULL,
                original_name TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                mime_type TEXT,
                version INTEGER DEFAULT 1,
                source TEXT DEFAULT 'upload',
                generated INTEGER DEFAULT 0,
                uploaded_by TEXT DEFAULT 'operator',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS inspection_reports (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                inspected_by TEXT NOT NULL,
                inspection_at TEXT NOT NULL,
                occupancy TEXT DEFAULT 'owner_occupied',
                condition_rating TEXT DEFAULT 'sound',
                summary TEXT NOT NULL,
                notes TEXT,
                approved INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS price_guidance_logs (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                version INTEGER DEFAULT 1,
                estimate_low INTEGER,
                estimate_high INTEGER,
                rationale TEXT,
                comparables TEXT DEFAULT '[]',
                quoted_channel TEXT,
                quoted_to TEXT,
                quoted_at TEXT,
                approved_by TEXT,
                approved_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS offer_events (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                amount INTEGER NOT NULL,
                buyer_name TEXT,
                conditions TEXT,
                channel TEXT DEFAULT 'manual',
                status TEXT DEFAULT 'received',
                received_at TEXT NOT NULL,
                notes TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS signing_sessions (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                token TEXT NOT NULL,
                status TEXT DEFAULT 'drafted',
                authority_pack_document_id TEXT,
                sent_to TEXT,
                signer_name TEXT,
                signer_email TEXT,
                signer_ip TEXT,
                signer_user_agent TEXT,
                sent_at TEXT,
                viewed_at TEXT,
                signed_at TEXT,
                serviced_at TEXT,
                archive_path TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
                id TEXT PRIMARY KEY,
                source_root TEXT NOT NULL,
                requested_by TEXT DEFAULT 'operator',
                worker_host TEXT,
                status TEXT DEFAULT 'started',
                total_files INTEGER DEFAULT 0,
                total_bytes INTEGER DEFAULT 0,
                scanned_files INTEGER DEFAULT 0,
                uploaded_files INTEGER DEFAULT 0,
                skipped_files INTEGER DEFAULT 0,
                failed_files INTEGER DEFAULT 0,
                uploaded_bytes INTEGER DEFAULT 0,
                last_heartbeat_at TEXT,
                error_summary TEXT,
                stats TEXT DEFAULT '{}',
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mirrored_assets (
                id TEXT PRIMARY KEY,
                sync_run_id TEXT,
                relative_path TEXT NOT NULL UNIQUE,
                directory_path TEXT DEFAULT '',
                original_name TEXT NOT NULL,
                storage_path TEXT,
                extension TEXT DEFAULT '',
                mime_type TEXT,
                size_bytes INTEGER DEFAULT 0,
                sha256 TEXT,
                modified_at TEXT,
                category TEXT DEFAULT 'other',
                preview_kind TEXT DEFAULT 'none',
                sensitivity TEXT DEFAULT 'standard',
                is_sensitive INTEGER DEFAULT 0,
                upload_status TEXT DEFAULT 'pending',
                text_extract_status TEXT DEFAULT 'not_started',
                text_extract_excerpt TEXT,
                attributes TEXT DEFAULT '{}',
                last_seen_at TEXT,
                uploaded_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS rea_api_logs (
                id TEXT PRIMARY KEY,
                lead_id TEXT,
                rea_upload_id TEXT,
                rea_listing_id TEXT,
                action TEXT NOT NULL,
                request_method TEXT,
                request_path TEXT,
                request_payload TEXT,
                response_status_code INTEGER,
                response_body TEXT,
                ok INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS asset_derivatives (
                id TEXT PRIMARY KEY,
                asset_id TEXT NOT NULL,
                derivative_type TEXT NOT NULL,
                storage_path TEXT,
                mime_type TEXT,
                size_bytes INTEGER DEFAULT 0,
                status TEXT DEFAULT 'ready',
                content_text TEXT,
                attributes TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_evidence_links (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                link_type TEXT DEFAULT 'archive_file',
                confidence_score INTEGER DEFAULT 70,
                rationale TEXT,
                include_on_lead INTEGER DEFAULT 1,
                linked_by TEXT DEFAULT 'operator',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS call_log (
                id TEXT PRIMARY KEY,
                lead_id TEXT DEFAULT '',
                lead_address TEXT DEFAULT '',
                user_id TEXT DEFAULT 'Shahid',
                outcome TEXT NOT NULL,
                connected INTEGER DEFAULT 0,
                timestamp TEXT DEFAULT '',
                call_duration_seconds INTEGER DEFAULT 0,
                duration_seconds INTEGER DEFAULT 0,
                note TEXT DEFAULT '',
                operator TEXT DEFAULT 'Shahid',
                logged_at TEXT DEFAULT '',
                logged_date TEXT DEFAULT '',
                next_action_due TEXT,
                provider TEXT DEFAULT 'manual',
                provider_call_id TEXT,
                direction TEXT DEFAULT '',
                from_number TEXT DEFAULT '',
                to_number TEXT DEFAULT '',
                raw_payload TEXT DEFAULT '{}',
                recording_url TEXT,
                recording_status TEXT,
                recording_duration_seconds INTEGER,
                transcript TEXT,
                summary TEXT,
                intent_signal REAL DEFAULT 0,
                booking_attempted INTEGER DEFAULT 0,
                next_step_detected INTEGER DEFAULT 0,
                objection_tags TEXT DEFAULT '[]'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS call_analysis (
                call_id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                provider TEXT DEFAULT 'zoom_phone',
                recording_id TEXT DEFAULT '',
                ai_call_summary_id TEXT DEFAULT '',
                status TEXT DEFAULT 'completed',
                summary TEXT DEFAULT '',
                outcome TEXT DEFAULT 'unknown',
                key_topics TEXT DEFAULT '[]',
                objections TEXT DEFAULT '[]',
                next_step TEXT DEFAULT '',
                suggested_follow_up_task TEXT DEFAULT '',
                sentiment_label TEXT DEFAULT '',
                sentiment_confidence REAL DEFAULT 0,
                sentiment_reason TEXT DEFAULT '',
                overall_confidence REAL DEFAULT 0,
                error_message TEXT DEFAULT '',
                raw_payload TEXT DEFAULT '{}',
                analyzed_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS opportunity_actions (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                action TEXT NOT NULL,
                expires_at TEXT,
                detector_key TEXT,
                note TEXT,
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS missions (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                command_text TEXT NOT NULL,
                objective TEXT DEFAULT '',
                target_type TEXT DEFAULT 'portfolio',
                target_id TEXT,
                target_label TEXT DEFAULT '',
                requested_by TEXT DEFAULT 'operator',
                trigger_source TEXT DEFAULT 'operator_command',
                trigger_reason TEXT DEFAULT '',
                status TEXT DEFAULT 'draft',
                priority TEXT DEFAULT 'normal',
                latest_run_id TEXT,
                fact_pack_hash TEXT DEFAULT '',
                budget_class TEXT DEFAULT 'heuristic_first',
                cache_hit INTEGER DEFAULT 0,
                llm_call_count INTEGER DEFAULT 0,
                director_summary TEXT,
                consensus_plan TEXT,
                recommended_steps TEXT DEFAULT '[]',
                department_statuses TEXT DEFAULT '[]',
                context_snapshot TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT,
                approved_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mission_runs (
                id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL,
                run_number INTEGER DEFAULT 1,
                status TEXT DEFAULT 'running',
                started_at TEXT,
                completed_at TEXT,
                objective_snapshot TEXT DEFAULT '',
                director_summary TEXT,
                consensus_plan TEXT,
                recommended_steps TEXT DEFAULT '[]',
                context_snapshot TEXT DEFAULT '{}',
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS mission_events (
                id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                sequence_no INTEGER DEFAULT 0,
                department TEXT DEFAULT 'director',
                role TEXT DEFAULT 'director',
                event_type TEXT DEFAULT 'note',
                status TEXT DEFAULT 'completed',
                title TEXT NOT NULL,
                summary TEXT DEFAULT '',
                detail TEXT DEFAULT '',
                evidence_refs TEXT DEFAULT '[]',
                payload TEXT DEFAULT '{}',
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS org_runs (
                id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                status TEXT DEFAULT 'queued',
                current_phase TEXT DEFAULT 'queued',
                autonomy_mode TEXT DEFAULT 'approve_sends_code',
                root_agent_id TEXT,
                summary TEXT DEFAULT '',
                metrics TEXT DEFAULT '{}',
                queued_at TEXT,
                started_at TEXT,
                heartbeat_at TEXT,
                completed_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_nodes (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                parent_id TEXT,
                name TEXT NOT NULL,
                agent_type TEXT DEFAULT 'specialist',
                department TEXT DEFAULT 'Director',
                role TEXT DEFAULT 'agent',
                model TEXT DEFAULT 'heuristic',
                capability_tags TEXT DEFAULT '[]',
                status TEXT DEFAULT 'queued',
                queue_name TEXT DEFAULT 'control',
                current_task TEXT DEFAULT '',
                depth INTEGER DEFAULT 0,
                spawned_children INTEGER DEFAULT 0,
                lease_expires_at TEXT,
                last_heartbeat_at TEXT,
                attributes TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS agent_heartbeats (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                status TEXT DEFAULT 'running',
                queue_name TEXT DEFAULT 'control',
                current_task TEXT DEFAULT '',
                detail TEXT DEFAULT '',
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS work_items (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                assigned_agent_id TEXT,
                department TEXT DEFAULT 'Director',
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                work_type TEXT DEFAULT 'analysis',
                status TEXT DEFAULT 'queued',
                priority TEXT DEFAULT 'normal',
                queue_name TEXT DEFAULT 'control',
                execution_mode TEXT DEFAULT 'rules',
                confidence REAL DEFAULT 0,
                expected_booking_lift REAL DEFAULT 0,
                approval_required INTEGER DEFAULT 0,
                depends_on_ids TEXT DEFAULT '[]',
                artifact_refs TEXT DEFAULT '[]',
                payload TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT,
                completed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS debate_sessions (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                title TEXT NOT NULL,
                topic TEXT DEFAULT '',
                status TEXT DEFAULT 'queued',
                consensus_summary TEXT DEFAULT '',
                dissent_summary TEXT DEFAULT '',
                created_at TEXT,
                updated_at TEXT,
                completed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS debate_turns (
                id TEXT PRIMARY KEY,
                debate_id TEXT NOT NULL,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                agent_id TEXT,
                department TEXT DEFAULT 'Director',
                role TEXT DEFAULT 'agent',
                stance TEXT DEFAULT 'proposal',
                claim_type TEXT DEFAULT 'proposal',
                content TEXT DEFAULT '',
                evidence_refs TEXT DEFAULT '[]',
                turn_index INTEGER DEFAULT 0,
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS review_gates (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                work_item_id TEXT,
                gate_type TEXT DEFAULT 'execution_review',
                title TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                requested_by TEXT DEFAULT 'system',
                approved_by TEXT,
                rejected_by TEXT,
                rationale TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT,
                approved_at TEXT,
                rejected_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS policy_versions (
                id TEXT PRIMARY KEY,
                org_run_id TEXT,
                version_no INTEGER DEFAULT 1,
                title TEXT NOT NULL,
                status TEXT DEFAULT 'proposed',
                summary TEXT DEFAULT '',
                active INTEGER DEFAULT 0,
                change_set TEXT DEFAULT '{}',
                created_at TEXT,
                approved_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS run_artifacts (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                artifact_type TEXT DEFAULT 'note',
                title TEXT NOT NULL,
                status TEXT DEFAULT 'ready',
                content TEXT DEFAULT '',
                attributes TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS execution_attempts (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                work_item_id TEXT,
                agent_id TEXT,
                expert_key TEXT DEFAULT '',
                execution_role TEXT DEFAULT '',
                provider TEXT DEFAULT '',
                model_alias TEXT DEFAULT '',
                model_name TEXT DEFAULT '',
                status TEXT DEFAULT 'queued',
                prompt_hash TEXT DEFAULT '',
                output_artifact_id TEXT,
                retry_count INTEGER DEFAULT 0,
                input_tokens INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                cost_band TEXT DEFAULT 'medium',
                metadata TEXT DEFAULT '{}',
                started_at TEXT,
                completed_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS control_triggers (
                id TEXT PRIMARY KEY,
                trigger_type TEXT DEFAULT 'operator_command',
                trigger_source TEXT DEFAULT 'operator',
                entity_type TEXT DEFAULT 'portfolio',
                entity_id TEXT DEFAULT '',
                status TEXT DEFAULT 'queued',
                priority TEXT DEFAULT 'normal',
                dedupe_key TEXT DEFAULT '',
                reason TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                fact_pack_hash TEXT DEFAULT '',
                mission_id TEXT,
                cooldown_until TEXT,
                created_at TEXT,
                processed_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_packs (
                id TEXT PRIMARY KEY,
                entity_type TEXT DEFAULT 'portfolio',
                entity_id TEXT DEFAULT '',
                scope TEXT DEFAULT 'control',
                fact_pack_hash TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                source_updated_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS improvement_candidates (
                id TEXT PRIMARY KEY,
                org_run_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                team TEXT DEFAULT 'Growth Team',
                title TEXT NOT NULL,
                status TEXT DEFAULT 'proposed',
                priority TEXT DEFAULT 'normal',
                summary TEXT DEFAULT '',
                expected_booking_lift REAL DEFAULT 0,
                confidence REAL DEFAULT 0,
                guardrail_risk TEXT DEFAULT 'low',
                payload TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT,
                approved_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS learning_evaluations (
                id TEXT PRIMARY KEY,
                evaluation_type TEXT DEFAULT 'bookings_first',
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                scorecard TEXT DEFAULT '{}',
                deltas TEXT DEFAULT '{}',
                summary TEXT DEFAULT '',
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_channel_consents (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                channel TEXT NOT NULL,
                purpose TEXT NOT NULL,
                status TEXT DEFAULT 'unknown',
                basis TEXT DEFAULT '',
                source TEXT DEFAULT 'operator',
                note TEXT,
                recipient TEXT DEFAULT '',
                recorded_by TEXT DEFAULT 'operator',
                recorded_at TEXT,
                expires_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_suppressions (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                channel TEXT DEFAULT 'all',
                status TEXT DEFAULT 'active',
                reason TEXT DEFAULT '',
                source TEXT DEFAULT 'operator',
                note TEXT,
                created_by TEXT DEFAULT 'operator',
                created_at TEXT,
                released_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_funnels (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                funnel_type TEXT NOT NULL,
                stage TEXT DEFAULT 'lead_captured',
                status TEXT DEFAULT 'active',
                owner TEXT DEFAULT 'operator',
                summary TEXT,
                next_step_title TEXT DEFAULT '',
                next_step_due_at TEXT,
                booked_at TEXT,
                completed_at TEXT,
                metrics TEXT DEFAULT '{}',
                created_at TEXT,
                updated_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_funnel_events (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                funnel_id TEXT NOT NULL,
                funnel_type TEXT NOT NULL,
                event_type TEXT DEFAULT 'note',
                title TEXT NOT NULL,
                detail TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                created_at TEXT
            )
            """,
        ]

        listing_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_listing_documents_lead_kind ON listing_documents(lead_id, kind)",
            "CREATE INDEX IF NOT EXISTS idx_inspection_reports_lead ON inspection_reports(lead_id, inspection_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_price_guidance_lead ON price_guidance_logs(lead_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_offer_events_lead ON offer_events(lead_id, received_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_signing_sessions_lead ON signing_sessions(lead_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_signing_sessions_token ON signing_sessions(token)",
            "CREATE INDEX IF NOT EXISTS idx_sync_runs_status ON sync_runs(status, started_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_mirrored_assets_sync ON mirrored_assets(sync_run_id, uploaded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_mirrored_assets_sha ON mirrored_assets(sha256)",
            "CREATE INDEX IF NOT EXISTS idx_mirrored_assets_sensitive ON mirrored_assets(is_sensitive, uploaded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_asset_derivatives_asset ON asset_derivatives(asset_id, derivative_type)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_evidence_unique ON lead_evidence_links(lead_id, asset_id, link_type)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_logged_date ON call_log(logged_date)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_lead ON call_log(lead_id, logged_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_lead_timestamp ON call_log(lead_id, timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_user ON call_log(user_id, logged_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_timestamp ON call_log(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_call_log_provider ON call_log(provider, logged_date DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_call_log_provider_unique ON call_log(provider, provider_call_id)",
            "CREATE INDEX IF NOT EXISTS idx_call_recording_status ON call_log(recording_status)",
            "CREATE INDEX IF NOT EXISTS idx_source_health_status ON source_health(status, blocked_until)",
            "CREATE INDEX IF NOT EXISTS idx_call_analysis_lead ON call_analysis(lead_id, analyzed_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_call_analysis_status ON call_analysis(status, analyzed_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_missions_created ON missions(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_missions_target ON missions(target_type, target_id)",
            "CREATE INDEX IF NOT EXISTS idx_missions_status ON missions(status, priority)",
            "CREATE INDEX IF NOT EXISTS idx_missions_trigger ON missions(trigger_source, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_missions_fact_pack ON missions(fact_pack_hash, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_mission_runs_mission ON mission_runs(mission_id, run_number DESC)",
            "CREATE INDEX IF NOT EXISTS idx_mission_events_run ON mission_events(run_id, sequence_no)",
            "CREATE INDEX IF NOT EXISTS idx_mission_events_mission ON mission_events(mission_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_org_runs_mission ON org_runs(mission_id, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_org_runs_status ON org_runs(status, current_phase)",
            "CREATE INDEX IF NOT EXISTS idx_agent_nodes_run ON agent_nodes(org_run_id, parent_id, depth)",
            "CREATE INDEX IF NOT EXISTS idx_agent_nodes_status ON agent_nodes(status, department)",
            "CREATE INDEX IF NOT EXISTS idx_agent_heartbeats_agent ON agent_heartbeats(agent_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_work_items_run ON work_items(org_run_id, status, priority)",
            "CREATE INDEX IF NOT EXISTS idx_work_items_agent ON work_items(assigned_agent_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_debate_sessions_run ON debate_sessions(org_run_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_debate_turns_debate ON debate_turns(debate_id, turn_index)",
            "CREATE INDEX IF NOT EXISTS idx_review_gates_run ON review_gates(org_run_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_policy_versions_status ON policy_versions(status, version_no DESC)",
            "CREATE INDEX IF NOT EXISTS idx_run_artifacts_run ON run_artifacts(org_run_id, artifact_type)",
            "CREATE INDEX IF NOT EXISTS idx_execution_attempts_run ON execution_attempts(org_run_id, status, started_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_execution_attempts_item ON execution_attempts(work_item_id, expert_key, started_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_control_triggers_dedupe ON control_triggers(dedupe_key)",
            "CREATE INDEX IF NOT EXISTS idx_control_triggers_status ON control_triggers(status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_control_triggers_entity ON control_triggers(entity_type, entity_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_fact_packs_entity ON fact_packs(entity_type, entity_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_fact_packs_hash ON fact_packs(fact_pack_hash)",
            "CREATE INDEX IF NOT EXISTS idx_improvement_candidates_run ON improvement_candidates(org_run_id, status, priority)",
            "CREATE INDEX IF NOT EXISTS idx_improvement_candidates_mission ON improvement_candidates(mission_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_learning_evaluations_created ON learning_evaluations(created_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_channel_consents_unique ON lead_channel_consents(lead_id, channel, purpose)",
            "CREATE INDEX IF NOT EXISTS idx_lead_channel_consents_status ON lead_channel_consents(status, recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_lead_suppressions_active ON lead_suppressions(lead_id, channel, status)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_funnels_unique ON lead_funnels(lead_id, funnel_type)",
            "CREATE INDEX IF NOT EXISTS idx_lead_funnels_stage ON lead_funnels(funnel_type, stage, status)",
            "CREATE INDEX IF NOT EXISTS idx_lead_funnel_events_funnel ON lead_funnel_events(funnel_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_lead_funnel_events_lead ON lead_funnel_events(lead_id, created_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS propella_waitlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                suburb_interest TEXT,
                offer_code TEXT DEFAULT '3_guides_bundle',
                phone TEXT,
                agency TEXT,
                team_size TEXT,
                message TEXT,
                submitted_at TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_propella_waitlist_submitted ON propella_waitlist(submitted_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS enrichment_jobs (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'cotality',
                status TEXT NOT NULL DEFAULT 'queued',
                requested_fields_json TEXT NOT NULL DEFAULT '[]',
                matched_address TEXT,
                machine_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                created_at TEXT,
                updated_at TEXT,
                completed_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS enrichment_results (
                id TEXT PRIMARY KEY,
                enrichment_job_id TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'cotality',
                raw_payload_json TEXT NOT NULL DEFAULT '{}',
                proposed_updates_json TEXT NOT NULL DEFAULT '{}',
                screenshot_path TEXT,
                confidence REAL,
                created_at TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_interactions (
                id TEXT PRIMARY KEY,
                lead_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                direction TEXT,
                summary TEXT DEFAULT '',
                payload_json TEXT DEFAULT '{}',
                actor TEXT,
                source TEXT,
                created_at TEXT
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_enrichment_jobs_status ON enrichment_jobs(status, created_at ASC)",
            "CREATE INDEX IF NOT EXISTS idx_enrichment_jobs_lead ON enrichment_jobs(lead_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_enrichment_results_job ON enrichment_results(enrichment_job_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_followup_due ON tasks(task_type, status, due_at)",
            "CREATE INDEX IF NOT EXISTS idx_lead_interactions_lead_created ON lead_interactions(lead_id, created_at DESC)",
            """
            CREATE TABLE IF NOT EXISTS rea_listing_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'land',
                headline_pattern TEXT NOT NULL,
                body_pattern TEXT NOT NULL,
                is_default BOOLEAN DEFAULT FALSE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """,
        ]

        for ddl in listing_tables:
            await conn.execute(text(ddl))

        # Seed default REA listing templates if table is empty.
        # SQLite migrations can partially fail on legacy files; guard this query
        # so DB boot does not abort before core lead tables are usable.
        try:
            _tpl_count_sq = (await conn.execute(text("SELECT COUNT(*) FROM rea_listing_templates"))).scalar_one()
        except Exception:
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rea_listing_templates (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        category TEXT NOT NULL DEFAULT 'land',
                        headline_pattern TEXT NOT NULL,
                        body_pattern TEXT NOT NULL,
                        is_default BOOLEAN DEFAULT FALSE,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            _tpl_count_sq = (await conn.execute(text("SELECT COUNT(*) FROM rea_listing_templates"))).scalar_one()
        if _tpl_count_sq == 0:
            _DISCLAIMER_SQ = (
                "Disclaimer: The above information has been gathered from sources that we believe are reliable."
                " However, Laing+Simmons real estate agency cannot guarantee the accuracy of this information"
                " and nor do we accept any responsibility for its accuracy. Any interested parties should rely"
                " on their own enquiries and judgment to determine the accuracy of this information for their own purposes."
            )
            _seed_tpls_sq = [
                {
                    "id": "tpl_first_home",
                    "name": "First Home Builder",
                    "category": "land",
                    "headline_pattern": "Build-Ready {land_size}sqm in {suburb}",
                    "body_pattern": (
                        "{principal_name} from {brand_name} is pleased to present Lot {lot_number}\n"
                        "\u2014 a {land_size} sqm registered block {position_phrase}.\n"
                        "{lot_type_line}\n\n"
                        "If you've been searching for the right block to build your first home, this one ticks every box.\n\n"
                        "Land size: {land_size} sqm\n"
                        "Frontage: {frontage}\n"
                        "{registration_line}\n\n"
                        "{location_block}\n\n"
                        "Choose your own builder or work with us to arrange a quality custom build\n"
                        "at a competitive price with build time guarantee.\n\n"
                        "To discuss this opportunity, contact {principal_name} on {principal_phone}\n"
                        "or email {principal_email}.\n\n"
                        "{disclaimer}"
                    ),
                },
                {
                    "id": "tpl_investor",
                    "name": "Investor Yield",
                    "category": "land",
                    "headline_pattern": "Titled {land_size}sqm Land \u2014 Lot {lot_number}, {suburb} | ${price}",
                    "body_pattern": (
                        "Nitin Puri from Laing+Simmons Oakville | Windsor presents "
                        "Lot {lot_number} \u2014 {land_size} sqm of titled land in {suburb}, one of "
                        "North West Sydney\u2019s highest-demand growth corridors. {lot_type_line}\n\n"
                        "New housing demand in {suburb} consistently outpaces supply. "
                        "Infrastructure spend is accelerating across the North West, and entry "
                        "pricing at ${price} sits well below the suburb\u2019s median house price. "
                        "Whether you\u2019re building to rent, holding for capital growth, or "
                        "constructing for owner-occupier resale, the fundamentals here are sound.\n\n"
                        "Key details:\n"
                        "\u2022 Land size: {land_size} sqm\n"
                        "\u2022 Frontage: {frontage}\n"
                        "\u2022 {registration_line}\n"
                        "\u2022 Price: ${price}\n\n"
                        "{location_block}\n\n"
                        "Multiple blocks available across this release. Contact us for the full "
                        "availability schedule and pricing across all lot sizes.\n\n"
                        "For enquiries, contact Nitin Puri on 0430 042 041 or email "
                        "oakville@lsre.com.au.\n\n"
                        + _DISCLAIMER_SQ
                    ),
                },
                {
                    "id": "tpl_family",
                    "name": "Family Upgrader",
                    "category": "land",
                    "headline_pattern": "{land_size}sqm Family Block in {suburb} \u2014 Lot {lot_number} | ${price}",
                    "body_pattern": (
                        "Nitin Puri from Laing+Simmons Oakville | Windsor is delighted to present "
                        "Lot {lot_number} \u2014 {land_size} sqm in {suburb}, sized for the family "
                        "that needs room to grow. {lot_type_line}\n\n"
                        "{frontage} of street frontage. {land_size} square metres to work with. "
                        "Enough space for a proper backyard, a double garage, a home office, and "
                        "bedrooms the kids won\u2019t outgrow in two years. No strata. No shared "
                        "walls. Your land, your architect, your rules.\n\n"
                        "Key details:\n"
                        "\u2022 Land size: {land_size} sqm\n"
                        "\u2022 Frontage: {frontage}\n"
                        "\u2022 {registration_line}\n"
                        "\u2022 Price: ${price}\n\n"
                        "{location_block}\n\n"
                        "Build with your own team or work with us to arrange a quality custom "
                        "build at a competitive price with build time guarantee.\n\n"
                        "To book a site visit or discuss your options, contact Nitin Puri "
                        "on 0430 042 041 or email oakville@lsre.com.au.\n\n"
                        + _DISCLAIMER_SQ
                    ),
                },
            ]
            for tpl in _seed_tpls_sq:
                await conn.execute(text(
                    "INSERT OR IGNORE INTO rea_listing_templates (id, name, category, headline_pattern, body_pattern, is_default)"
                    " VALUES (:id, :name, :category, :headline_pattern, :body_pattern, 1)"
                ), tpl)

        control_alters = [
            ("missions", "trigger_source", "TEXT DEFAULT 'operator_command'"),
            ("missions", "trigger_reason", "TEXT DEFAULT ''"),
            ("missions", "fact_pack_hash", "TEXT DEFAULT ''"),
            ("missions", "budget_class", "TEXT DEFAULT 'heuristic_first'"),
            ("missions", "cache_hit", "INTEGER DEFAULT 0"),
            ("missions", "llm_call_count", "INTEGER DEFAULT 0"),
            ("work_items", "execution_mode", "TEXT DEFAULT 'rules'"),
            ("work_items", "confidence", "REAL DEFAULT 0"),
            ("work_items", "expected_booking_lift", "REAL DEFAULT 0"),
            ("work_items", "capability_requirement", "TEXT DEFAULT 'cheap_small_text'"),
            ("work_items", "escalation_level", "INTEGER DEFAULT 0"),
            ("work_items", "retry_count", "INTEGER DEFAULT 0"),
            ("work_items", "input_context_summary", "TEXT DEFAULT ''"),
            ("work_items", "output_summary", "TEXT DEFAULT ''"),
            ("leads", "notes", "TEXT"),
            ("leads", "estimated_completion", "TEXT"),
            ("leads", "follow_up_due_at", "TEXT"),
            ("leads", "preferred_contact_method", "TEXT DEFAULT ''"),
            ("leads", "followup_frequency", "TEXT DEFAULT 'none'"),
            ("leads", "market_updates_opt_in", "INTEGER DEFAULT 0"),
            ("leads", "next_followup_at", "TEXT"),
            ("leads", "followup_status", "TEXT DEFAULT 'active'"),
            ("leads", "followup_notes", "TEXT"),
            ("leads", "last_activity_type", "TEXT DEFAULT ''"),
            ("tasks", "payload_json", "TEXT DEFAULT '{}'"),
            ("tasks", "attempt_count", "INTEGER DEFAULT 0"),
            ("tasks", "last_error", "TEXT"),
            ("call_analysis", "analyzed_at", "TEXT"),
            ("call_analysis", "created_at", "TEXT"),
            ("call_analysis", "updated_at", "TEXT"),
            ("call_log", "user_id", "TEXT"),
            ("call_log", "timestamp", "TEXT"),
            ("call_log", "logged_date", "TEXT"),
            ("call_log", "next_action_due", "TEXT"),
            ("call_log", "provider", "TEXT"),
            ("call_log", "provider_call_id", "TEXT"),
            ("call_log", "direction", "TEXT"),
            ("call_log", "from_number", "TEXT"),
            ("call_log", "to_number", "TEXT"),
            ("call_log", "raw_payload", "TEXT"),
            ("call_log", "recording_url", "TEXT"),
            ("call_log", "recording_status", "TEXT"),
            ("call_log", "recording_duration_seconds", "INTEGER"),
            ("call_log", "transcript", "TEXT"),
            ("call_log", "summary", "TEXT"),
            ("call_log", "intent_signal", "REAL DEFAULT 0"),
            ("call_log", "booking_attempted", "INTEGER DEFAULT 0"),
            ("call_log", "next_step_detected", "INTEGER DEFAULT 0"),
            ("call_log", "objection_tags", "TEXT DEFAULT '[]'"),
        ]
        for table_name, col_name, col_def in control_alters:
            try:
                await conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}"))
            except Exception:
                pass

        for ddl in listing_indexes:
            await conn.execute(text(ddl))

        await ensure_revenue_engine_schema(conn)

        for col_name, col_type in new_columns:
            try:
                await conn.execute(text(f"ALTER TABLE leads ADD COLUMN {col_name} {col_type}"))
            except Exception:
                pass  # column already exists

        # Migrate leads: add contacts columns if not present
        def migrate_leads(sync_conn):
            for col in [("contacts", "JSONB DEFAULT '[]'"), ("potential_contacts", "JSONB DEFAULT '[]'")]:
                try:
                    sync_conn.execute(text(f"ALTER TABLE leads ADD COLUMN {col[0]} {col[1]}"))
                except Exception:
                    pass
        
        await conn.run_sync(migrate_leads)

        # Migrate email_accounts: add new rotation columns if not present
        email_acct_cols = [
            ("daily_cap",        "INTEGER NOT NULL DEFAULT 80"),
            ("is_warmup_mode",   "INTEGER NOT NULL DEFAULT 0"),
            ("warmup_day",       "INTEGER NOT NULL DEFAULT 0"),
            ("sends_today",      "INTEGER NOT NULL DEFAULT 0"),
            ("sends_today_date", "TEXT DEFAULT ''"),
            ("is_active",        "INTEGER NOT NULL DEFAULT 1"),
        ]
        for col_name, col_def in email_acct_cols:
            try:
                await conn.execute(text(f"ALTER TABLE email_accounts ADD COLUMN {col_name} {col_def}"))
            except Exception:
                pass
        for idx_name, tbl, col_name in new_indexes:
            try:
                await conn.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {tbl}({col_name})"))
            except Exception:
                pass  # index already exists or table not present yet

        # Ensure all SQLModel tables (including distress_sources, etc.) are created
        # side-effect: registers tables in metadata
        from models import (  # noqa: F401
            archive_models, control_models, distress_models, funnel_models, 
            intelligence_models, sql_models
        )
        
        # SQLite doesn't support schemas. Temporarily clear them for creation.
        original_schemas = {}
        for table in SQLModel.metadata.tables.values():
            if table.schema:
                original_schemas[table.name] = table.schema
                table.schema = None
        
        await conn.run_sync(SQLModel.metadata.create_all)

        # Restore schemas for any subsequent Postgres logic
        for table_name, schema in original_schemas.items():
            if table_name in SQLModel.metadata.tables:
                SQLModel.metadata.tables[table_name].schema = schema

    _checkpoint_sqlite_wal_best_effort(core.config.DB_PATH)

async def init_intelligence_schema():
    """
    Create `intelligence` schema and all intelligence.* tables.
    Safely migrates existing `leads` rows into intelligence.property + intelligence.event.
    Called in main.py lifespan alongside init_postgres().
    """
    import uuid as _uuid
    from models.intelligence_models import (  # noqa: F401 — side-effect: registers tables
        IntelligenceProperty, IntelligenceParty,
        IntelligencePropertyParty, IntelligenceEvent, IntelligenceMedia,
    )
    from models.sql_models import Lead as LeadModel

    async with async_engine.begin() as conn:
        # Ensure schema exists first so table creation succeeds on Postgres.
        # SQLite uses ATTACH DATABASE compatibility in the engine connect hook.
        if not core.config.DATABASE_URL.startswith("sqlite"):
            try:
                await conn.execute(text("CREATE SCHEMA IF NOT EXISTS intelligence"))
            except Exception:
                pass
        # Create all intelligence.* tables (no-op if they already exist)
        await conn.run_sync(SQLModel.metadata.create_all)

    try:
        async_session = sessionmaker(
            async_engine, class_=AsyncSession, expire_on_commit=False
        )
        async with async_session() as session:
            # Idempotency guard
            result = await session.execute(
                text("SELECT COUNT(*) FROM intelligence.property")
            )
            if result.scalar_one() > 0:
                return  # already migrated

            leads_result = await session.execute(select(LeadModel))
            leads = leads_result.scalars().all()
            now = now_iso()
            for lead in leads:
                prop_id = hashlib.md5(lead.address.encode()).hexdigest()
                await session.execute(
                    text("""
                        INSERT INTO intelligence.property
                            (id, address, suburb, postcode, lat, lng, est_value,
                             zoning_type, parcel_details, last_settlement_date,
                             trigger_type, status, route_queue, heat_score,
                             evidence_score, h3index, cadid, created_at, updated_at)
                        VALUES
                            (:id, :address, :suburb, :postcode, :lat, :lng, :est_value,
                             :zoning_type, :parcel_details, :last_settlement_date,
                             :trigger_type, :status, :route_queue, :heat_score,
                             :evidence_score, :h3index, :cadid, :created_at, :updated_at)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {
                        "id": prop_id,
                        "address": lead.address,
                        "suburb": lead.suburb,
                        "postcode": lead.postcode,
                        "lat": lead.lat,
                        "lng": lead.lng,
                        "est_value": lead.est_value,
                        "zoning_type": lead.zoning_type,
                        "parcel_details": lead.parcel_details,
                        "last_settlement_date": getattr(lead, "last_settlement_date", None),
                        "trigger_type": lead.trigger_type,
                        "status": lead.status,
                        "route_queue": getattr(lead, "route_queue", ""),
                        "heat_score": lead.heat_score,
                        "evidence_score": lead.evidence_score,
                        "h3index": getattr(lead, "h3index", None),
                        "cadid": getattr(lead, "cadid", None),
                        "created_at": lead.created_at,
                        "updated_at": lead.updated_at,
                    }
                )
                # Seed a LISTED event for each migrated lead
                await session.execute(
                    text("""
                        INSERT INTO intelligence.event
                            (id, property_id, event_type, source, raw_payload, occurred_at, created_at)
                        VALUES
                            (:id, :property_id, 'LISTED', 'migration', '{}', :occurred_at, :created_at)
                        ON CONFLICT (id) DO NOTHING
                    """),
                    {
                        "id": str(_uuid.uuid4()),
                        "property_id": prop_id,
                        "occurred_at": lead.created_at,
                        "created_at": now,
                    }
                )
            await session.commit()
            print(f"[Intelligence] Migrated {len(leads)} leads → intelligence.property")
    except Exception as e:
        print(f"[Intelligence] Migration skipped or failed: {e}")


# ─── Compatibility helpers (SQLite-era helpers still used by some routes) ─────

def _sqlmodel_to_dict(obj) -> dict:
    """Convert a SQLModel instance or mapping to a plain dict."""
    if obj is None:
        return {}
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return dict(obj)


def _task_to_dict(row) -> dict:
    return _sqlmodel_to_dict(row)


def _appointment_to_dict(row) -> dict:
    return _sqlmodel_to_dict(row)


def _sold_event_to_dict(row) -> dict:
    return _sqlmodel_to_dict(row)


async def _get_lead_or_404(session_or_conn, lead_id: str):
    """Fetch a lead by id using AsyncSession; raise 404 if not found."""
    from fastapi import HTTPException
    from sqlalchemy import text as _text
    try:
        result = await session_or_conn.execute(
            _text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id}
        )
        row = result.mappings().first()
    except Exception:
        row = None
    if not row:
        raise HTTPException(status_code=404, detail="Lead not found")
    return dict(row)


async def _fetch_joined_task(session_or_conn, task_id: str) -> dict:
    """Fetch a task joined with its lead using AsyncSession."""
    from sqlalchemy import text as _text
    result = await session_or_conn.execute(
        _text("""
            SELECT t.*, l.address, l.suburb, l.owner_name
            FROM tasks t
            LEFT JOIN leads l ON t.lead_id = l.id
            WHERE t.id = :task_id
        """),
        {"task_id": task_id},
    )
    row = result.mappings().first()
    return dict(row) if row else {}


# Stub _connect for files that import it but don't actually call it
class _ConnectStub:
    """No-op context manager for legacy imports of _connect."""
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass
    async def execute(self, *args, **kwargs):
        raise RuntimeError("_connect is deprecated — use get_session() instead")

def _connect():
    return _ConnectStub()
