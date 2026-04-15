from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from platformdirs import user_state_dir
except ImportError:  # pragma: no cover - optional dependency until installed everywhere
    user_state_dir = None

DEFAULT_API_KEY = "HILLS_SECURE_2026_CORE"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
LOCAL_SERVICE_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1", "db", "redis", "backend", "frontend"}

BACKEND_ROOT = Path(__file__).resolve().parents[1]
_CANDIDATE_PROJECT_ROOT = BACKEND_ROOT.parent
PROJECT_ROOT = (
    _CANDIDATE_PROJECT_ROOT
    if (_CANDIDATE_PROJECT_ROOT / "frontend").exists() or (_CANDIDATE_PROJECT_ROOT / ".git").exists()
    else BACKEND_ROOT
)
ENV_FILE = BACKEND_ROOT / ".env"
LEGACY_STOCK_ROOT = Path(r"D:\L+S Stock")


def _default_runtime_root() -> Path:
    if user_state_dir:
        return Path(user_state_dir("woonona-lead-machine", "OpenAI"))
    return PROJECT_ROOT / ".runtime"


def _is_local_base_url(value: str) -> bool:
    lowered = value.strip().lower()
    return any(host in lowered for host in ("localhost", "127.0.0.1", "0.0.0.0"))


def _hostname_from_url(value: str | None) -> str:
    try:
        return (urlparse((value or "").strip()).hostname or "").strip().lower()
    except ValueError:
        return ""


def _is_local_service_url(value: str | None) -> bool:
    return _hostname_from_url(value) in LOCAL_SERVICE_HOSTS


def _is_sqlite_database_url(value: str | None) -> bool:
    return ((value or "").strip().lower()).startswith("sqlite")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8-sig",
        extra="ignore",
        case_sensitive=False,
    )

    app_env: str = Field(default="development", validation_alias="APP_ENV")
    api_key: str = Field(default="HILLS_SECURE_2026_CORE", validation_alias="API_KEY")
    app_title: str = Field(default="Property Intelligence Core", validation_alias="APP_TITLE")
    use_postgres: bool = Field(default=True, validation_alias="USE_POSTGRES")
    database_url: str | None = Field(default=None, validation_alias="DATABASE_URL")
    redis_url: str = Field(default=DEFAULT_REDIS_URL, validation_alias="REDIS_URL")

    stock_root: Path | None = Field(default=None, validation_alias="STOCK_ROOT")
    mirror_storage_root: Path | None = Field(default=None, validation_alias="MIRROR_STORAGE_ROOT")
    mirror_text_max_chars: int = Field(default=24000, validation_alias="MIRROR_TEXT_MAX_CHARS")
    temp_dir: Path | None = Field(default=None, validation_alias="TEMP_DIR")

    brand_principal_name: str = Field(default="Nitin Puri", validation_alias="BRAND_PRINCIPAL_NAME")
    brand_principal_email: str = Field(default="oakville@lsre.com.au", validation_alias="BRAND_PRINCIPAL_EMAIL")
    brand_principal_phone: str = Field(default="0430 042 041", validation_alias="BRAND_PRINCIPAL_PHONE")
    default_operator_name: str = Field(default="System Operator", validation_alias="DEFAULT_OPERATOR_NAME")

    ownit1st_operator_name: str = Field(default="Shahid", validation_alias="OWNIT1ST_OPERATOR_NAME")
    ownit1st_email: str = Field(default="info@ownit1stloans.com.au", validation_alias="OWNIT1ST_EMAIL")
    ownit1st_phone: str = Field(default="04 85 85 7881", validation_alias="OWNIT1ST_PHONE")
    ownit1st_website: str = Field(default="https://ownit1stloans.com.au/", validation_alias="OWNIT1ST_WEBSITE")

    base_url: str = Field(
        default_factory=lambda: os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8001"),
        validation_alias="BASE_URL",
    )
    frontend_url: str | None = Field(default=None, validation_alias="FRONTEND_URL")
    mapbox_access_token: str = Field(default="", validation_alias="MAPBOX_ACCESS_TOKEN")

    domain_client_id: str = Field(default="", validation_alias="DOMAIN_CLIENT_ID")
    domain_client_secret: str = Field(default="", validation_alias="DOMAIN_CLIENT_SECRET")
    domain_api_key: str = Field(default="", validation_alias="DOMAIN_API_KEY")

    rea_client_id: str = Field(default="", validation_alias="REA_CLIENT_ID")
    rea_client_secret: str = Field(default="", validation_alias="REA_CLIENT_SECRET")
    rea_agency_id: str = Field(default="", validation_alias="REA_AGENCY_ID")
    rea_auto_publish_enabled: bool = Field(default=False, validation_alias="REA_AUTO_PUBLISH_ENABLED")
    rea_require_explicit_push_confirmation: bool = Field(
        default=True,
        validation_alias="REA_REQUIRE_EXPLICIT_PUSH_CONFIRMATION",
    )

    gemini_api_key: str = Field(default="", validation_alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-flash-latest", validation_alias="GEMINI_MODEL")
    gemini_rpm_cap: int = Field(default=12, validation_alias="GEMINI_RPM_CAP")

    newsapi_key: str = Field(default="", validation_alias="NEWSAPI_KEY")
    background_send_poll_seconds: int = Field(default=30, validation_alias="BACKGROUND_SEND_POLL_SECONDS")
    primary_strike_suburb: str = Field(default="Bligh Park", validation_alias="PRIMARY_STRIKE_SUBURB")
    secondary_strike_suburbs_raw: str = Field(
        default="South Windsor,Oakville,Windsor",
        validation_alias="SECONDARY_STRIKE_SUBURBS",
    )

    run_heavy_background_loops: bool = Field(default=False, validation_alias="RUN_HEAVY_BACKGROUND_LOOPS")

    @model_validator(mode="after")
    def finalize(self) -> "Settings":
        if not self.database_url:
            raise ValueError("DATABASE_URL must be set.")
        self.use_postgres = not _is_sqlite_database_url(self.database_url)

        if self.app_env == "production":
            if _is_sqlite_database_url(self.database_url):
                raise ValueError("DATABASE_URL must point to managed Postgres when APP_ENV is production")
            if _is_local_service_url(self.database_url):
                raise ValueError("DATABASE_URL must point to managed Postgres when APP_ENV is production")
            if not self.redis_url:
                raise ValueError("REDIS_URL must be set when APP_ENV is production")
            if _is_local_service_url(self.redis_url):
                raise ValueError("REDIS_URL must point to managed Redis when APP_ENV is production")
            if not self.base_url:
                raise ValueError("BASE_URL must be set when APP_ENV is production")
            if _is_local_base_url(str(self.base_url)):
                raise ValueError("BASE_URL must point to a public URL when APP_ENV is production")
            if not self.frontend_url:
                raise ValueError("FRONTEND_URL must be set when APP_ENV is production")
            if _is_local_base_url(self.frontend_url):
                raise ValueError("FRONTEND_URL must point to a public URL when APP_ENV is production")
            if self.api_key == DEFAULT_API_KEY:
                raise ValueError("API_KEY must be set to a non-default value when APP_ENV is production")

        if self.stock_root is None:
            if self.app_env == "development":
                self.stock_root = LEGACY_STOCK_ROOT
            else:
                self.stock_root = PROJECT_ROOT / "_stock_archive"

        if self.mirror_storage_root is None:
            self.mirror_storage_root = PROJECT_ROOT / "mirror_storage"
        if self.temp_dir is None:
            self.temp_dir = _default_runtime_root() / "tmp"

        self.mirror_text_max_chars = max(4000, int(self.mirror_text_max_chars))
        self.background_send_poll_seconds = max(15, int(self.background_send_poll_seconds))
        return self

    @property
    def db_path(self) -> str:
        # Migration/backfill tools may still need the old SQLite artifact path.
        return str(PROJECT_ROOT / "leads.db")

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT

    @property
    def project_log_path(self) -> Path:
        return PROJECT_ROOT / "PROJECT_VISION_LOG.md"

    @property
    def secondary_strike_suburbs(self) -> list[str]:
        return [item.strip() for item in self.secondary_strike_suburbs_raw.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
