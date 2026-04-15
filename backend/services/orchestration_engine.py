"""
Orchestration Engine — provider abstraction, routing, retry, circuit-breaking.

Providers supported:
  nim     — NVIDIA NIM (OpenAI-compatible, env: NVIDIA_API_KEY)
  gemini  — Google Gemini Flash (env: GEMINI_API_KEY)
  claude  — Anthropic via openai-compat proxy or direct (env: ANTHROPIC_API_KEY)
  ollama  — Local Ollama (env: OLLAMA_BASE_URL, default http://localhost:11434)

Routing: cheapest capable model first, escalate on failure/rate-limit.
Each provider has a circuit breaker — opens after 3 consecutive failures,
stays open for CIRCUIT_RESET_SECONDS then half-opens for probe.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx

from core.events import event_manager

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

CIRCUIT_RESET_SECONDS = int(os.getenv("ORCH_CIRCUIT_RESET_SECONDS", "120"))
MAX_RETRIES_PER_PROVIDER = 2
REQUEST_TIMEOUT = float(os.getenv("ORCH_REQUEST_TIMEOUT", "60"))

# ─── Work type → capability requirements ──────────────────────────────────────

WORK_TYPE_CAPABILITIES: Dict[str, List[str]] = {
    "implementation":   ["code_generation", "file_editing"],
    "debugging":        ["code_generation", "reasoning"],
    "test_fixing":      ["code_generation"],
    "architecture":     ["reasoning", "long_context"],
    "review":           ["code_review", "reasoning"],
    "ui_polish":        ["code_generation"],
    "documentation":    ["text_generation"],
    "research":         ["reasoning", "long_context"],
    "refactor":         ["code_generation"],
    "outreach_copy":    ["text_generation"],
    "classification":   ["classification"],
    "summarization":    ["text_generation"],
    "default":          [],
}

# ─── Provider registry ────────────────────────────────────────────────────────

class ProviderConfig:
    def __init__(
        self,
        key: str,
        display_name: str,
        base_url: str,
        api_key: str,
        default_model: str,
        capabilities: List[str],
        rpm_cap: int,
        cost_per_1k_input: float,   # USD
        cost_per_1k_output: float,
        is_openai_compat: bool = True,
    ):
        self.key = key
        self.display_name = display_name
        self.base_url = base_url
        self.api_key = api_key
        self.default_model = default_model
        self.capabilities = capabilities
        self.rpm_cap = rpm_cap
        self.cost_per_1k_input = cost_per_1k_input
        self.cost_per_1k_output = cost_per_1k_output
        self.is_openai_compat = is_openai_compat

    @property
    def available(self) -> bool:
        return bool(self.api_key)


def _build_provider_registry() -> Dict[str, ProviderConfig]:
    return {
        "nim": ProviderConfig(
            key="nim",
            display_name="NVIDIA NIM",
            base_url=os.getenv("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1"),
            api_key=os.getenv("NVIDIA_API_KEY", ""),
            default_model=os.getenv("NIM_MODEL", "meta/llama-3.3-70b-instruct"),
            capabilities=["code_generation", "text_generation", "reasoning", "classification"],
            rpm_cap=int(os.getenv("NIM_RPM_CAP", "30")),
            cost_per_1k_input=0.0004,
            cost_per_1k_output=0.0004,
        ),
        "gemini": ProviderConfig(
            key="gemini",
            display_name="Gemini Flash",
            base_url="https://generativelanguage.googleapis.com/v1beta/openai",
            api_key=os.getenv("GEMINI_API_KEY", ""),
            default_model=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
            capabilities=["code_generation", "text_generation", "reasoning", "classification", "long_context"],
            rpm_cap=int(os.getenv("GEMINI_RPM_CAP", "12")),
            cost_per_1k_input=0.00015,
            cost_per_1k_output=0.0006,
        ),
        "claude": ProviderConfig(
            key="claude",
            display_name="Claude (Anthropic)",
            base_url="https://api.anthropic.com/v1",
            api_key=os.getenv("ANTHROPIC_API_KEY", ""),
            default_model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6"),
            capabilities=["code_generation", "file_editing", "text_generation", "reasoning",
                          "code_review", "long_context"],
            rpm_cap=int(os.getenv("CLAUDE_RPM_CAP", "10")),
            cost_per_1k_input=0.003,
            cost_per_1k_output=0.015,
            is_openai_compat=False,  # uses Anthropic Messages API
        ),
        "ollama": ProviderConfig(
            key="ollama",
            display_name="Ollama (Local)",
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            api_key="ollama",  # always "available" — no API key needed
            default_model=os.getenv("OLLAMA_MODEL", "mistral"),
            capabilities=["code_generation", "text_generation", "classification"],
            rpm_cap=999,
            cost_per_1k_input=0.0,
            cost_per_1k_output=0.0,
        ),
    }


# ─── Per-provider runtime state ───────────────────────────────────────────────

class _ProviderRuntime:
    """In-memory rate limiting and circuit breaker per provider."""

    def __init__(self, rpm_cap: int):
        self.rpm_cap = rpm_cap
        self._lock = asyncio.Lock()
        self._request_timestamps: List[float] = []
        self._consecutive_failures = 0
        self._circuit_open = False
        self._circuit_open_until: Optional[float] = None
        self.status = "healthy"

    async def acquire(self) -> bool:
        """Returns True if the request is allowed now."""
        async with self._lock:
            now = time.monotonic()
            # Check circuit breaker
            if self._circuit_open:
                if now < (self._circuit_open_until or 0):
                    return False
                # Half-open: allow one probe
                self._circuit_open = False
                self._circuit_open_until = None
                logger.info("[circuit] half-open probe")

            # Sliding window RPM check
            cutoff = now - 60.0
            self._request_timestamps = [t for t in self._request_timestamps if t > cutoff]
            if len(self._request_timestamps) >= self.rpm_cap:
                self.status = "rate_limited"
                return False

            self._request_timestamps.append(now)
            self.status = "healthy"
            return True

    def record_success(self):
        self._consecutive_failures = 0
        self.status = "healthy"

    def record_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= 3:
            self._circuit_open = True
            self._circuit_open_until = time.monotonic() + CIRCUIT_RESET_SECONDS
            self.status = "unavailable"
            logger.warning("[circuit] OPEN — too many failures")
        else:
            self.status = "degraded"

    @property
    def is_circuit_open(self) -> bool:
        return self._circuit_open


# ─── Global state ─────────────────────────────────────────────────────────────

_PROVIDERS: Dict[str, ProviderConfig] = {}
_RUNTIMES: Dict[str, _ProviderRuntime] = {}


def init_providers():
    """Call once at startup to build provider registry."""
    global _PROVIDERS, _RUNTIMES
    _PROVIDERS = _build_provider_registry()
    _RUNTIMES = {k: _ProviderRuntime(p.rpm_cap) for k, p in _PROVIDERS.items()}
    logger.info("Orchestration providers: %s", list(_PROVIDERS.keys()))


def get_provider_snapshot() -> List[Dict[str, Any]]:
    """Return serializable health snapshot for all providers."""
    out = []
    for key, cfg in _PROVIDERS.items():
        rt = _RUNTIMES.get(key)
        out.append({
            "key": key,
            "display_name": cfg.display_name,
            "available": cfg.available,
            "status": rt.status if rt else "unknown",
            "circuit_open": rt.is_circuit_open if rt else False,
            "rpm_cap": cfg.rpm_cap,
            "default_model": cfg.default_model,
            "capabilities": cfg.capabilities,
        })
    return out


# ─── Provider selection ───────────────────────────────────────────────────────

def _rank_providers(work_type: str, preferred: Optional[str] = None) -> List[str]:
    """
    Return ordered provider list for a work type.
    Cheapest capable first.  Preferred provider moved to front if capable.
    """
    required_caps = WORK_TYPE_CAPABILITIES.get(work_type, [])
    candidates = []
    for key, cfg in _PROVIDERS.items():
        if not cfg.available:
            continue
        if required_caps and not any(c in cfg.capabilities for c in required_caps):
            continue
        rt = _RUNTIMES[key]
        if rt.is_circuit_open:
            continue
        # Cost sort key
        cost = cfg.cost_per_1k_input + cfg.cost_per_1k_output
        candidates.append((cost, key))
    candidates.sort(key=lambda x: x[0])
    order = [k for _, k in candidates]
    if preferred and preferred in order:
        order.remove(preferred)
        order.insert(0, preferred)
    return order


# ─── Completion helpers ───────────────────────────────────────────────────────

async def _call_openai_compat(
    cfg: ProviderConfig,
    messages: List[Dict],
    model: Optional[str],
    max_tokens: int = 4096,
) -> Tuple[str, int, int]:
    """OpenAI-compatible /chat/completions. Returns (text, input_tokens, output_tokens)."""
    url = f"{cfg.base_url.rstrip('/')}/chat/completions"
    payload = {
        "model": model or cfg.default_model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        r = await client.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    content = data["choices"][0]["message"]["content"]
    usage = data.get("usage", {})
    return content, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)


async def _call_ollama(
    cfg: ProviderConfig,
    messages: List[Dict],
    model: Optional[str],
    max_tokens: int = 2048,
) -> Tuple[str, int, int]:
    """Ollama /api/chat endpoint."""
    url = f"{cfg.base_url.rstrip('/')}/api/chat"
    payload = {
        "model": model or cfg.default_model,
        "messages": messages,
        "stream": False,
        "options": {"num_predict": max_tokens},
    }
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
    content = data.get("message", {}).get("content", "")
    prompt_tokens = data.get("prompt_eval_count", 0)
    output_tokens = data.get("eval_count", 0)
    return content, prompt_tokens, output_tokens


async def _call_anthropic(
    cfg: ProviderConfig,
    messages: List[Dict],
    model: Optional[str],
    max_tokens: int = 4096,
) -> Tuple[str, int, int]:
    """Anthropic Messages API."""
    url = "https://api.anthropic.com/v1/messages"
    # Convert OpenAI-style messages to Anthropic format
    system_content = ""
    anthro_messages = []
    for m in messages:
        if m["role"] == "system":
            system_content += m["content"] + "\n"
        else:
            anthro_messages.append({"role": m["role"], "content": m["content"]})

    payload: Dict[str, Any] = {
        "model": model or cfg.default_model,
        "max_tokens": max_tokens,
        "messages": anthro_messages,
    }
    if system_content:
        payload["system"] = system_content.strip()

    headers = {
        "x-api-key": cfg.api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        r = await client.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    content = data["content"][0]["text"]
    usage = data.get("usage", {})
    return content, usage.get("input_tokens", 0), usage.get("output_tokens", 0)


async def _dispatch_to_provider(
    provider_key: str,
    messages: List[Dict],
    model: Optional[str] = None,
    max_tokens: int = 4096,
) -> Tuple[str, int, int]:
    """Route to the correct call helper for the given provider."""
    cfg = _PROVIDERS[provider_key]
    if provider_key == "ollama":
        return await _call_ollama(cfg, messages, model, max_tokens)
    elif provider_key == "claude" and not cfg.is_openai_compat:
        return await _call_anthropic(cfg, messages, model, max_tokens)
    else:
        return await _call_openai_compat(cfg, messages, model, max_tokens)


# ─── Main routing entry point ─────────────────────────────────────────────────

class RouteResult:
    def __init__(self, text: str, provider: str, model: str,
                 input_tokens: int, output_tokens: int, cost_usd: float,
                 fallbacks_used: List[str]):
        self.text = text
        self.provider = provider
        self.model = model
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cost_usd = cost_usd
        self.fallbacks_used = fallbacks_used

    def to_dict(self) -> Dict[str, Any]:
        return {
            "text": self.text,
            "provider": self.provider,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": self.cost_usd,
            "fallbacks_used": self.fallbacks_used,
        }


async def route_completion(
    work_type: str,
    messages: List[Dict],
    preferred_provider: Optional[str] = None,
    model_override: Optional[str] = None,
    max_tokens: int = 4096,
    job_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> RouteResult:
    """
    Route a completion request across providers with automatic fallback.
    Tries providers in cost-ascending order, skipping circuit-open / rate-limited.
    Emits WebSocket events for live UI.
    """
    if not _PROVIDERS:
        init_providers()

    chain = _rank_providers(work_type, preferred_provider)
    if not chain:
        raise RuntimeError("No providers available for work_type: " + work_type)

    fallbacks_used: List[str] = []
    last_error: Optional[Exception] = None

    for provider_key in chain:
        rt = _RUNTIMES[provider_key]
        cfg = _PROVIDERS[provider_key]

        allowed = await rt.acquire()
        if not allowed:
            fallbacks_used.append(provider_key)
            await event_manager.broadcast({
                "type": "ORCH_PROVIDER_SKIP",
                "data": {
                    "provider": provider_key,
                    "reason": "rate_limited" if rt.status == "rate_limited" else "circuit_open",
                    "job_id": job_id,
                    "task_id": task_id,
                    "ts": datetime.utcnow().isoformat(),
                }
            })
            continue

        try:
            await event_manager.broadcast({
                "type": "ORCH_PROVIDER_CALL",
                "data": {
                    "provider": provider_key,
                    "model": model_override or cfg.default_model,
                    "work_type": work_type,
                    "job_id": job_id,
                    "task_id": task_id,
                    "ts": datetime.utcnow().isoformat(),
                }
            })

            text, in_tok, out_tok = await _dispatch_to_provider(
                provider_key, messages, model_override or None, max_tokens
            )
            rt.record_success()

            # Cost estimate
            cost = (in_tok / 1000 * cfg.cost_per_1k_input +
                    out_tok / 1000 * cfg.cost_per_1k_output)

            await event_manager.broadcast({
                "type": "ORCH_PROVIDER_DONE",
                "data": {
                    "provider": provider_key,
                    "model": model_override or cfg.default_model,
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                    "cost_usd": cost,
                    "fallbacks_used": fallbacks_used,
                    "job_id": job_id,
                    "task_id": task_id,
                    "ts": datetime.utcnow().isoformat(),
                }
            })

            return RouteResult(
                text=text,
                provider=provider_key,
                model=model_override or cfg.default_model,
                input_tokens=in_tok,
                output_tokens=out_tok,
                cost_usd=cost,
                fallbacks_used=fallbacks_used,
            )

        except Exception as exc:
            rt.record_failure()
            fallbacks_used.append(provider_key)
            last_error = exc
            logger.warning("[orch] provider %s failed: %s", provider_key, exc)

            await event_manager.broadcast({
                "type": "ORCH_PROVIDER_FAIL",
                "data": {
                    "provider": provider_key,
                    "error": str(exc)[:200],
                    "circuit_open": rt.is_circuit_open,
                    "job_id": job_id,
                    "task_id": task_id,
                    "ts": datetime.utcnow().isoformat(),
                }
            })
            continue

    raise RuntimeError(
        f"All providers exhausted for work_type={work_type}. "
        f"Last error: {last_error}. Chain tried: {chain}"
    )
