import os
import asyncio
import datetime as _dt
import sqlite3
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from fastapi.security.api_key import APIKeyHeader

BACKEND_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_PROJECT_ROOT = BACKEND_ROOT.parent
PROJECT_ROOT = (
    _CANDIDATE_PROJECT_ROOT
    if (_CANDIDATE_PROJECT_ROOT / "frontend").exists() or (_CANDIDATE_PROJECT_ROOT / ".git").exists()
    else BACKEND_ROOT
)

try:
    load_dotenv(dotenv_path=BACKEND_ROOT / ".env", encoding="utf-8-sig")
except UnicodeDecodeError:
    # Some local env files have legacy Windows-1252 bytes; fall back so startup
    # does not fail before the app can read explicit environment variables.
    load_dotenv(dotenv_path=BACKEND_ROOT / ".env", encoding="cp1252")

DEFAULT_API_KEY = "HILLS_SECURE_2026_CORE"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
LOCAL_SERVICE_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1", "db", "redis", "backend", "frontend"}
API_KEY = os.getenv("API_KEY", DEFAULT_API_KEY)
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

APP_TITLE = "Property Intelligence Core"


def _sqlite_has_leads_table(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        with sqlite3.connect(str(path)) as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='leads' LIMIT 1"
            ).fetchone()
            return bool(row)
    except Exception:
        return False


def _default_sqlite_db_path() -> Path:
    preferred = [
        PROJECT_ROOT / "databases" / "leads.db",
        PROJECT_ROOT / "leads.db",
        BACKEND_ROOT / "leads.db",
    ]
    for candidate in preferred:
        if _sqlite_has_leads_table(candidate):
            return candidate
    for candidate in preferred:
        if candidate.exists():
            return candidate
    return PROJECT_ROOT / "leads.db"


DB_PATH = str(_default_sqlite_db_path())
APP_ENV = (os.getenv("APP_ENV") or "development").strip().lower()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    if APP_ENV == "production":
        DATABASE_URL = ""
    else:
        import logging as _cfg_log_mod
        _cfg_log_mod.getLogger("config").warning(
            "DATABASE_URL not set - defaulting to local SQLite. "
            "Set DATABASE_URL env var for Postgres."
        )
        DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"
USE_POSTGRES = not DATABASE_URL.lower().startswith("sqlite")
if not USE_POSTGRES:
    sqlite_path = DATABASE_URL.split("sqlite+aiosqlite:///", 1)[-1].strip() or DB_PATH
    DB_PATH = sqlite_path
REDIS_URL = os.getenv("REDIS_URL", DEFAULT_REDIS_URL)

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
RUNTIME_ROLE = (os.getenv("RUNTIME_ROLE") or "web").strip().lower()
if RUNTIME_ROLE not in {"web", "worker", "scheduler"}:
    import logging as _cfg_log_mod2
    _cfg_log_mod2.getLogger("config").warning("Unsupported RUNTIME_ROLE %r — defaulting to 'web'", RUNTIME_ROLE)
    RUNTIME_ROLE = "web"
DEFAULT_STOCK_ROOT = (
    r"D:\L+S Stock" if APP_ENV == "development" and os.name == "nt" else str(PROJECT_ROOT / "_stock_archive")
)
STOCK_ROOT = Path(os.getenv("STOCK_ROOT", DEFAULT_STOCK_ROOT))
MIRROR_STORAGE_ROOT = Path(os.getenv("MIRROR_STORAGE_ROOT", str(PROJECT_ROOT / "mirror_storage")))
MIRROR_TEXT_MAX_CHARS = max(4000, int(os.getenv("MIRROR_TEXT_MAX_CHARS", "24000")))
TEMP_DIR = Path(os.getenv("TEMP_DIR", str(PROJECT_ROOT / "tmp")))
GENERATED_REPORTS_ROOT = Path(os.getenv("GENERATED_REPORTS_ROOT", str(BACKEND_ROOT / "generated_reports")))
STREETVIEW_IMAGE_ROOT = Path(os.getenv("STREETVIEW_IMAGE_ROOT", str(BACKEND_ROOT / "streetview_images")))
RECORDINGS_ROOT = Path(os.getenv("RECORDINGS_ROOT", str(BACKEND_ROOT / "recordings")))
LISTING_PHOTOS_ROOT = Path(os.getenv("LISTING_PHOTOS_ROOT", str(BACKEND_ROOT / "listing_photos")))
SMS_BRIDGE_URL = (os.getenv("SMS_BRIDGE_URL") or "").strip().rstrip("/")
RUN_BACKGROUND_LOOPS = (
    os.getenv("RUN_BACKGROUND_LOOPS", "true" if APP_ENV == "development" else "false").strip().lower() == "true"
)
SPEECH_AUDIO_STORAGE_ROOT = Path(os.getenv("SPEECH_AUDIO_STORAGE_ROOT", str(PROJECT_ROOT / "speech_audio")))
SPEECH_TRANSCRIPTION_PROVIDER = os.getenv("SPEECH_TRANSCRIPTION_PROVIDER", "stub_transcription")
SPEECH_DIARIZATION_PROVIDER = os.getenv("SPEECH_DIARIZATION_PROVIDER", "stub_diarization")
SPEECH_FEATURE_EXTRACTOR_PROVIDER = os.getenv("SPEECH_FEATURE_EXTRACTOR_PROVIDER", "heuristic_feature_extractor")
SPEECH_SALES_ANALYSIS_PROVIDER = os.getenv("SPEECH_SALES_ANALYSIS_PROVIDER", "heuristic_sales_analysis")
SPEECH_COACHING_PROVIDER = os.getenv("SPEECH_COACHING_PROVIDER", "heuristic_coaching")
SPEECH_ORCHESTRATOR_BACKEND = os.getenv("SPEECH_ORCHESTRATOR_BACKEND", "inline_stub")
SPEECH_LIVEKIT_BACKEND = os.getenv("SPEECH_LIVEKIT_BACKEND", "")
SPEECH_TEMPORAL_BACKEND = os.getenv("SPEECH_TEMPORAL_BACKEND", "")
SPEECH_VECTOR_BACKEND = os.getenv("SPEECH_VECTOR_BACKEND", "")
SPEECH_ANALYTICS_BACKEND = os.getenv("SPEECH_ANALYTICS_BACKEND", "")
PROJECT_LOG_PATH = PROJECT_ROOT / "PROJECT_VISION_LOG.md"
BRAND_NAME = "Laing+Simmons Oakville | Windsor"
BRAND_AREA = "Oakville | Windsor"
BRAND_LOGO_URL = "https://images.squarespace-cdn.com/content/v1/52c0bc66e4b035c2f1f884fc/1473065644926-35LTYYPWD09N2SLU311T/Laing%2B%2B%2BSimmons%2BLogo.jpg"
# ─── L+S Principal Identity ──────────────────────────────────────────────────
PRINCIPAL_NAME = os.getenv("BRAND_PRINCIPAL_NAME", "Nitin Puri")
PRINCIPAL_EMAIL = os.getenv("BRAND_PRINCIPAL_EMAIL", "oakville@lsre.com.au")
PRINCIPAL_PHONE = os.getenv("BRAND_PRINCIPAL_PHONE", "0430 042 041")
DEFAULT_OPERATOR_NAME = os.getenv("DEFAULT_OPERATOR_NAME", "System Operator")

# ─── Ownit1st Brand Identity ─────────────────────────────────────────────────
OWNIT1ST_OPERATOR_NAME = os.getenv("OWNIT1ST_OPERATOR_NAME", "Shahid")
OWNIT1ST_EMAIL = os.getenv("OWNIT1ST_EMAIL", "info@ownit1stloans.com.au")
OWNIT1ST_PHONE = os.getenv("OWNIT1ST_PHONE", "04 85 85 7881")
OWNIT1ST_WEBSITE = os.getenv("OWNIT1ST_WEBSITE", "https://ownit1stloans.com.au/")
OWNIT1ST_BRAND_NAME = "Ownit1st Loans"

BASE_URL = os.getenv("BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "http://localhost:8001"
FRONTEND_URL = (os.getenv("FRONTEND_URL") or "").strip().rstrip("/")
_RAW_ALLOWED_ORIGIN_REGEX = (os.getenv("ALLOWED_ORIGIN_REGEX") or "").strip() or None

def _is_local_base_url(value: str) -> bool:
    lowered = value.lower()
    return any(host in lowered for host in ("localhost", "127.0.0.1", "0.0.0.0"))


def _hostname_from_url(value: str) -> str:
    try:
        return (urlparse(value).hostname or "").strip().lower()
    except ValueError:
        return ""


def _is_local_service_url(value: str) -> bool:
    return _hostname_from_url(value) in LOCAL_SERVICE_HOSTS


def _is_sqlite_database_url(value: str) -> bool:
    return value.strip().lower().startswith("sqlite")

def _split_csv_env(value: str) -> list[str]:
    return [item.strip().rstrip("/") for item in (value or "").split(",") if item.strip()]


def get_allowed_origins() -> list[str]:
    origins: list[str] = []
    origins.extend(_split_csv_env(os.getenv("ALLOWED_ORIGINS", "")))
    if FRONTEND_URL:
        origins.append(FRONTEND_URL)
    if APP_ENV == "development" or _is_local_base_url(BASE_URL):
        origins.extend(
            [
                "http://localhost:3000",
                "http://localhost:5173",
                "http://localhost:5174",
                "http://localhost:9000",
                "http://127.0.0.1:3000",
                "http://127.0.0.1:5173",
                "http://127.0.0.1:5174",
                "http://127.0.0.1:9000",
                "https://localhost:5174",
                "https://127.0.0.1:5174",
            ]
        )
    return list(dict.fromkeys(origins))


def get_allowed_origin_regex() -> Optional[str]:
    if _RAW_ALLOWED_ORIGIN_REGEX:
        return _RAW_ALLOWED_ORIGIN_REGEX
    if APP_ENV == "development" or _is_local_base_url(BASE_URL):
        return r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$"
    return None


ALLOWED_ORIGINS = get_allowed_origins()
ALLOWED_ORIGIN_REGEX = get_allowed_origin_regex()


def build_public_url(path: str) -> str:
    text = (path or "").strip()
    if not text or text.startswith(("http://", "https://")):
        return text
    normalized = text if text.startswith("/") else f"/{text}"
    return f"{BASE_URL.rstrip('/')}{normalized}"


import logging as _config_logging
_config_log = _config_logging.getLogger("config")

# Production environment validation — warn instead of crash so the app can
# still boot and serve /health even when config is incomplete.  This prevents
# Render restart loops that make debugging impossible.
_PROD_CONFIG_WARNINGS: list[str] = []

if API_KEY == DEFAULT_API_KEY and not _is_local_base_url(BASE_URL):
    _PROD_CONFIG_WARNINGS.append("API_KEY is still the default value for a non-local deployment.")

if APP_ENV == "production":
    _critical_checks: list[tuple[bool, str]] = [
        (not os.getenv("DATABASE_URL"), "DATABASE_URL must be set for production."),
        (_is_sqlite_database_url(DATABASE_URL), "DATABASE_URL must not point to SQLite in production."),
        (_is_local_service_url(DATABASE_URL), "DATABASE_URL must point to managed Postgres, not a local host."),
    ]
    _critical_errors = [msg for condition, msg in _critical_checks if condition]
    if _critical_errors:
        raise RuntimeError("Invalid production configuration: " + "; ".join(_critical_errors))

    _checks: list[tuple[bool, str]] = [
        (not os.getenv("REDIS_URL"), "REDIS_URL is not set (defaulting to localhost - Redis features will be unavailable)."),
        (_is_local_service_url(REDIS_URL), "REDIS_URL points to a local host (Redis features will be unavailable)."),
        (not os.getenv("BASE_URL") and not os.getenv("RENDER_EXTERNAL_URL"), "BASE_URL is not set."),
        (_is_local_base_url(BASE_URL), "BASE_URL points to localhost."),
        (not os.getenv("FRONTEND_URL"), "FRONTEND_URL is not set."),
        (os.getenv("FRONTEND_URL") and _is_local_base_url(FRONTEND_URL), "FRONTEND_URL points to localhost."),
        (API_KEY == DEFAULT_API_KEY, "API_KEY is still the default value."),
    ]
    for condition, msg in _checks:
        if condition:
            _PROD_CONFIG_WARNINGS.append(msg)

for _w in _PROD_CONFIG_WARNINGS:
    _config_log.warning("PROD CONFIG: %s", _w)

# ─── Claude Agent SDK ────────────────────────────────────────────────────────
# Set ENABLE_SDK_AGENTS=true to route Tier 1 Hermes departments through
# autonomous multi-step agent loops (requires ANTHROPIC_API_KEY).
ENABLE_SDK_AGENTS = os.getenv("ENABLE_SDK_AGENTS", "false").lower() == "true"
CLAUDE_SDK_DAILY_BUDGET_USD = float(os.getenv("CLAUDE_SDK_DAILY_BUDGET_USD", "5.0"))
CLAUDE_SDK_SESSION_TOKEN_CAP = int(os.getenv("CLAUDE_SDK_SESSION_TOKEN_CAP", "50000"))

MAPBOX_ACCESS_TOKEN = os.getenv("MAPBOX_ACCESS_TOKEN", "")
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "")

# ─── Domain API ───────────────────────────────────────────────────────────────
DOMAIN_CLIENT_ID = os.getenv("DOMAIN_CLIENT_ID", "")
DOMAIN_CLIENT_SECRET = os.getenv("DOMAIN_CLIENT_SECRET", "")
DOMAIN_API_KEY = os.getenv("DOMAIN_API_KEY", "")
DOMAIN_CALLS_PER_DAY = 490  # leave 10 buffer from 500 limit
DOMAIN_SOURCE_403_THRESHOLD = max(1, int(os.getenv("DOMAIN_SOURCE_403_THRESHOLD", "3")))
DOMAIN_SOURCE_COOLDOWN_SECONDS = max(60, int(os.getenv("DOMAIN_SOURCE_COOLDOWN_SECONDS", "14400")))

# ─── REA Partner API (realestate.com.au) ──────────────────────────────────────
# Essential account — $399 AUD/month
# Set REA_CLIENT_SECRET by retrieving it once from the clientSecretLink URL
# provided in your REA welcome email, then paste the result here.
REA_CLIENT_ID = os.getenv("REA_CLIENT_ID", "")
REA_CLIENT_SECRET = os.getenv("REA_CLIENT_SECRET", "")
# The agency account ID assigned to Laing+Simmons Oakville | Windsor on REA.
# Find it in your REA Partner Portal account settings.
REA_AGENCY_ID = os.getenv("REA_AGENCY_ID", "")

# ─── Gemini ───────────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
KIMI_API_KEY = os.getenv("KIMI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_RPM_CAP = int(os.getenv("GEMINI_RPM_CAP", "12"))
PROBATE_LLM_EXTRACTION_ENABLED = os.getenv("PROBATE_LLM_EXTRACTION_ENABLED", "true").lower() == "true"
PROBATE_EXTRACT_MODEL = os.getenv("PROBATE_EXTRACT_MODEL", GEMINI_MODEL or "gemini-2.5-flash")
PROBATE_EXTRACT_FALLBACK_MODEL = os.getenv("PROBATE_EXTRACT_FALLBACK_MODEL", "gemini-2.5-pro")

# ─── NewsAPI ──────────────────────────────────────────────────────────────────
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")

PROJECT_MEMORY_RULE = "Non-negotiable: every user prompt for this project must be appended below with a Sydney timestamp so any future AI can reconstruct the vision and decision trail."
BACKGROUND_SEND_POLL_SECONDS = max(15, int(os.getenv("BACKGROUND_SEND_POLL_SECONDS", "30")))
_BACKGROUND_SENDER_TASK: Optional[asyncio.Task] = None

PRIMARY_STRIKE_SUBURB = os.getenv("PRIMARY_STRIKE_SUBURB", "Bligh Park")
SECONDARY_STRIKE_SUBURBS = [
    item.strip()
    for item in os.getenv("SECONDARY_STRIKE_SUBURBS", "South Windsor,Oakville,Windsor").split(",")
    if item.strip()
]

# ─── Dual-Locus Configuration ───────────────────────────────────────────────

# Locus 2765 (Box Hill / Melonba / Windsor / Oakville)
LOCUS_2765_SUBURBS = [
    "Box Hill", "Melonba", "Windsor", "Oakville", "South Windsor",
    "Riverstone", "Marsden Park", "Vineyard", "Gables", "McGraths Hill",
    "Bligh Park",
]

# Locus 2517 (Woonona / Bulli / Thirroul)
LOCUS_2517_SUBURBS = [
    "Woonona", "Bulli", "Thirroul", "Corrimal", "Bellambi", "Russell Vale",
]

ALL_TARGET_SUBURBS = LOCUS_2765_SUBURBS + LOCUS_2517_SUBURBS

# Agency feed targets — verified March 2026 via direct HTTP checks
# wp_json = WordPress REST API base URL (ingestor appends "posts" to fetch)
# rss     = RSS 2.0 feed URL
# Blank   = no public feed found (custom platform, 403, or no WP install)
#
# To add a new feed:  paste the URL and restart the backend.
# The REAXML ingestor polls every 15 min automatically.

AGENCY_FEEDS_2765: list = [
    # ── CONFIRMED WORKING ────────────────────────────────────────────────────
    {
        "name": "Stone Real Estate Hawkesbury",
        "wp_json": "https://www.stonehawkesbury.com.au/wp-json/wp/v2/",
        # Covers Box Hill → Windsor corridor; posts confirmed live
    },
    {
        "name": "Professionals Riverstone",
        "rss": "https://www.professionalsriverstone.com.au/feed/",
        # WordPress 6.9.4 confirmed; no blog posts yet but feed channel is live
    },

    # ── NO PUBLIC FEED (custom platform or franchise CMS) ────────────────────
    {"name": "Ray White TJG",              "wp_json": ""},  # Custom React app, no WP
    {"name": "Laing+Simmons Box Hill",     "reaxml":  ""},  # Contact franchise IT for REAXML
    {"name": "LJ Hooker Windsor",          "wp_json": ""},  # Proprietary LJ Hooker CMS
    {"name": "PRD Nationwide Windsor",     "reaxml":  ""},  # Contact PRD head office
    {"name": "McGrath Box Hill",           "wp_json": ""},  # Central mcgrath.com.au platform
    {"name": "Harcourts Marsden Park",     "wp_json": ""},  # harcourts.com.au blocks scraping
    {"name": "Century 21 Rouse Hill",      "wp_json": ""},  # C21 proprietary CMS
    {
        "name": "First National Connect Windsor",
        "wp_json": "",  # firstnationalconnect.com.au — WP likely but /wp-json returns 403
        # Try contacting them directly: admin@firstnationalconnect.com.au
    },
    {"name": "Richardson & Wrench Windsor","reaxml":  ""},  # Contact R&W head office
    {"name": "The Agency Gables/Box Hill", "wp_json": ""},  # Push Creative custom platform
    {"name": "Raine & Horne Windsor",      "reaxml":  ""},  # Served via Richmond office
    {"name": "Elders Windsor",             "reaxml":  ""},  # Contact Elders franchise IT
]

AGENCY_FEEDS_2517: list = [
    # ── CONFIRMED WORKING ────────────────────────────────────────────────────
    {
        "name": "McNeice X Woonona",
        "wp_json": "https://www.mcneice.com.au/wp-json/wp/v2/",
        # Posts confirmed live; most recent: Woonona duplex listing 2026-03-11
    },
    {
        "name": "Dignam Real Estate Woonona",
        "wp_json": "https://www.dignam.com.au/wp-json/wp/v2/",
        "rss": "https://www.dignam.com.au/feed/",
        # Both endpoints confirmed live; posts since 2024-03-04
    },
    {
        "name": "Stone Real Estate Illawarra",
        "wp_json": "https://www.stonerealestate.com.au/wp-json/wp/v2/",
        # Shared main-site WP; posts confirmed 2026-03-17
        # Note: filter by suburb in ingestor — all Stone offices share this endpoint
    },

    # ── NO PUBLIC FEED ────────────────────────────────────────────────────────
    {"name": "LJ Hooker Corrimal",    "wp_json": ""},  # wollongong.ljhooker.com.au — proprietary CMS
    {"name": "Ray White Thirroul",    "wp_json": ""},  # Ray White custom platform, no WP
    {"name": "Harcourts Wollongong",  "wp_json": ""},  # harcourtswollongong.com.au — 403
    {"name": "Century 21 Thirroul",   "wp_json": ""},  # No dedicated Thirroul office found
    {"name": "PRD Woonona-Bulli",     "wp_json": ""},  # ListOnce platform, no feed
    {
        "name": "First National Wollongong (Bulli)",
        "wp_json": "",  # fnwre.com.au — WP likely but /wp-json returns 403
    },
]

# ─── Routing Queues ──────────────────────────────────────────────────────────
QUEUE_RE = "real_estate"
QUEUE_MORTGAGE = "mortgage_ownit1st"
QUEUE_DEVELOPMENT = "development_acquisition"

# ─── Mortgage Cliff Detection ────────────────────────────────────────────────
# Settled 2.5–3.5 years ago → fixed-rate cliff window
MORTGAGE_CLIFF_MIN_YEARS = 2.5
MORTGAGE_CLIFF_MAX_YEARS = 3.5

# ─── NSW Public API Endpoints ────────────────────────────────────────────────
NSW_SPATIAL_FEATURESERVER_URL = (
    "https://portal.spatial.nsw.gov.au/server/rest/services"
    "/NSW_Land_Parcel_Property_Theme/FeatureServer/8/query"
)
NSW_GURAS_API_URL = (
    "https://maps.six.nsw.gov.au/arcgis/rest/services"
    "/sixmaps/LandBaseMap/MapServer/"   # stub — append layer/query params
)
NSW_EPLANNING_DA_URL = (
    "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineDA"
)

# ─── Cotality Cache TTLs ─────────────────────────────────────────────────────
# How long enriched data from the Cotality API is considered fresh before
# a background re-fetch is triggered.
COTALITY_TTLS: dict = {
    "property_report": _dt.timedelta(days=90),
    "suburb_profile": _dt.timedelta(days=30),
    "valuation": _dt.timedelta(days=14),
    "comparable_sales": _dt.timedelta(days=7),
}

# ─── Local Cotality Enrichment Runner ────────────────────────────────────────
COTALITY_BASE_URL = os.getenv("COTALITY_BASE_URL", "https://rpp.corelogic.com.au/")
COTALITY_PROFILE_DIR = os.getenv(
    "COTALITY_PROFILE_DIR",
    str(Path(__file__).resolve().parents[1] / "scripts" / ".cotality-profile"),
)
COTALITY_HEADLESS = os.getenv("COTALITY_HEADLESS", "true").lower() == "true"
COTALITY_LOGIN_WAIT_SECONDS = max(60, int(os.getenv("COTALITY_LOGIN_WAIT_SECONDS", "900")))
ENRICHMENT_MACHINE_ID = os.getenv("ENRICHMENT_MACHINE_ID", "local-cotality-runner")
ENRICHMENT_MACHINE_TOKEN = os.getenv("ENRICHMENT_MACHINE_TOKEN", "")
ENRICHMENT_MAX_JOBS_PER_HOUR = max(1, int(os.getenv("ENRICHMENT_MAX_JOBS_PER_HOUR", "12")))
ENRICHMENT_DELAY_MIN_MS = max(250, int(os.getenv("ENRICHMENT_DELAY_MIN_MS", "1500")))
ENRICHMENT_DELAY_MAX_MS = max(
    ENRICHMENT_DELAY_MIN_MS,
    int(os.getenv("ENRICHMENT_DELAY_MAX_MS", "6000")),
)
ENRICHMENT_COOLDOWN_AFTER_N_JOBS = max(1, int(os.getenv("ENRICHMENT_COOLDOWN_AFTER_N_JOBS", "5")))
ENRICHMENT_COOLDOWN_SECONDS = max(30, int(os.getenv("ENRICHMENT_COOLDOWN_SECONDS", "120")))
ENRICHMENT_API_BASE_URL = os.getenv("API_BASE_URL", BASE_URL)

# Door-knock sheet sync (CSV/XLSX). When set, scheduler mirrors door-knock leads
# bidirectionally between CRM and spreadsheet on the configured cadence.
DOOR_KNOCK_SYNC_FILE = os.getenv("DOOR_KNOCK_SYNC_FILE", "").strip()
DOOR_KNOCK_SYNC_POLL_SECONDS = max(5, int(os.getenv("DOOR_KNOCK_SYNC_POLL_SECONDS", "20")))


