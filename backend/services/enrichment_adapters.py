from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Protocol


@dataclass
class ProviderConfig:
    key: str
    enabled: bool = True
    feature_flag: str | None = None
    rate_limit_per_minute: int = 60
    cache_ttl_seconds: int = 900
    retry_attempts: int = 2
    timeout_seconds: float = 10.0
    secret_env_vars: list[str] = field(default_factory=list)

    def is_configured(self) -> bool:
        return all(os.getenv(name) for name in self.secret_env_vars)


class EnrichmentAdapter(Protocol):
    config: ProviderConfig

    async def enrich(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        ...


class AdapterRuntime:
    def __init__(self) -> None:
        self._cache: dict[str, Dict[str, Any]] = {}

    async def run(self, adapter: EnrichmentAdapter, cache_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        cached = self._cache.get(cache_key)
        if cached:
            return {"ok": True, "cached": True, **cached}
        last_error: str | None = None
        for _ in range(max(1, adapter.config.retry_attempts + 1)):
            try:
                result = await asyncio.wait_for(adapter.enrich(payload), timeout=adapter.config.timeout_seconds)
                self._cache[cache_key] = result
                return {"ok": True, "cached": False, **result}
            except Exception as exc:  # pragma: no cover - defensive wrapper
                last_error = str(exc)
        return {"ok": False, "cached": False, "error": last_error or "adapter_failed"}


runtime = AdapterRuntime()


DOMAIN_PROPERTY_PROVIDER = ProviderConfig(
    key="domain_property",
    feature_flag="provider.domain_property",
    rate_limit_per_minute=30,
    cache_ttl_seconds=900,
    retry_attempts=2,
    secret_env_vars=["DOMAIN_CLIENT_ID", "DOMAIN_CLIENT_SECRET"],
)

INFOTRACK_PERSON_PROVIDER = ProviderConfig(
    key="infotrack_person",
    feature_flag="provider.infotrack_person",
    rate_limit_per_minute=60,
    cache_ttl_seconds=3600,
    retry_attempts=1,
    secret_env_vars=["INFOTRACK_API_KEY"],
)

WHITEPAGES_PERSON_PROVIDER = ProviderConfig(
    key="whitepages_person",
    feature_flag="provider.whitepages_person",
    rate_limit_per_minute=30,
    cache_ttl_seconds=3600,
    retry_attempts=1,
    secret_env_vars=["WHITEPAGES_API_KEY"],
)
