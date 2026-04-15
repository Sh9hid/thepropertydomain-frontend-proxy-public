from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_core_models import ProviderUsageLog


def _flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    enabled: bool
    api_key_present: bool
    default_model: str
    daily_budget_usd: float


@dataclass(frozen=True)
class FeaturePolicy:
    enabled: bool
    cheap_order: tuple[str, ...]
    expensive_order: tuple[str, ...]


@dataclass(frozen=True)
class ProviderRoutingPolicy:
    providers: Dict[str, ProviderConfig]
    features: Dict[str, FeaturePolicy]
    default_feature: FeaturePolicy


@dataclass(frozen=True)
class RoutingDecision:
    allowed: bool
    provider: Optional[str]
    model: Optional[str]
    reason: str


def load_provider_routing_policy() -> ProviderRoutingPolicy:
    providers = {
        "gemini": ProviderConfig(
            name="gemini",
            enabled=_flag("AI_PROVIDER_GEMINI_ENABLED", True),
            api_key_present=bool(os.getenv("GEMINI_API_KEY", "").strip()),
            default_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            daily_budget_usd=float(os.getenv("AI_PROVIDER_GEMINI_DAILY_BUDGET_USD", "5")),
        ),
        "kimi": ProviderConfig(
            name="kimi",
            enabled=_flag("AI_PROVIDER_KIMI_ENABLED", True),
            api_key_present=bool(os.getenv("KIMI_API_KEY", "").strip()),
            default_model=os.getenv("KIMI_MODEL", "moonshot-v1-32k"),
            daily_budget_usd=float(os.getenv("AI_PROVIDER_KIMI_DAILY_BUDGET_USD", "3")),
        ),
        "codex": ProviderConfig(
            name="codex",
            enabled=_flag("AI_PROVIDER_CODEX_ENABLED", False),
            api_key_present=bool(os.getenv("OPENAI_API_KEY", "").strip()),
            default_model=os.getenv("OPENAI_MODEL", "gpt-5.4"),
            daily_budget_usd=float(os.getenv("AI_PROVIDER_CODEX_DAILY_BUDGET_USD", "0")),
        ),
    }

    default_feature = FeaturePolicy(
        enabled=_flag("AI_ROUTING_ENABLED", True),
        cheap_order=("kimi", "gemini"),
        expensive_order=("gemini", "kimi"),
    )
    features = {
        "light_drafting": FeaturePolicy(
            enabled=_flag("AI_FEATURE_LIGHT_DRAFTING_ENABLED", True),
            cheap_order=("kimi", "gemini"),
            expensive_order=("gemini", "kimi"),
        ),
        "call_summary": FeaturePolicy(
            enabled=_flag("AI_FEATURE_CALL_SUMMARY_ENABLED", True),
            cheap_order=("kimi", "gemini"),
            expensive_order=("gemini", "kimi"),
        ),
        "research_synthesis": FeaturePolicy(
            enabled=_flag("AI_FEATURE_RESEARCH_SYNTHESIS_ENABLED", True),
            cheap_order=("kimi", "gemini"),
            expensive_order=("gemini", "kimi"),
        ),
    }
    return ProviderRoutingPolicy(providers=providers, features=features, default_feature=default_feature)


def resolve_provider_for_feature(
    policy: ProviderRoutingPolicy,
    *,
    feature: str,
    task_class: str,
    spent_by_provider: Optional[Dict[str, float]] = None,
) -> RoutingDecision:
    feature_policy = policy.features.get(feature, policy.default_feature)
    if not feature_policy.enabled:
        return RoutingDecision(allowed=False, provider=None, model=None, reason="feature_disabled")

    order = feature_policy.expensive_order if task_class == "expensive" else feature_policy.cheap_order
    for provider_name in order:
        provider = policy.providers.get(provider_name)
        if provider is None or not provider.enabled:
            continue
        if not provider.api_key_present:
            continue
        spent = float((spent_by_provider or {}).get(provider.name, 0.0))
        if spent >= provider.daily_budget_usd:
            continue
        if provider.daily_budget_usd <= 0:
            continue
        return RoutingDecision(
            allowed=True,
            provider=provider.name,
            model=provider.default_model,
            reason="selected",
        )

    return RoutingDecision(allowed=False, provider=None, model=None, reason="no_provider_available")


async def get_daily_provider_spend(session: AsyncSession) -> Dict[str, float]:
    today = datetime.now(timezone.utc).date()
    rows = (
        await session.execute(
            select(
                ProviderUsageLog.provider,
                func.coalesce(func.sum(ProviderUsageLog.estimated_cost_usd), 0.0),
            )
            .where(func.date(ProviderUsageLog.created_at) == today)
            .group_by(ProviderUsageLog.provider)
        )
    ).all()
    return {str(provider): float(amount or 0.0) for provider, amount in rows}


async def log_provider_usage(
    session: AsyncSession,
    *,
    provider: str,
    model: Optional[str],
    feature: str,
    task_class: str,
    status: str,
    estimated_cost_usd: float = 0.0,
    usage_json: Optional[Dict[str, object]] = None,
    error_message: Optional[str] = None,
) -> ProviderUsageLog:
    row = ProviderUsageLog(
        provider=provider,
        model=model,
        feature=feature,
        task_class=task_class,
        status=status,
        estimated_cost_usd=estimated_cost_usd,
        usage_json=usage_json or {},
        error_message=error_message,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row
