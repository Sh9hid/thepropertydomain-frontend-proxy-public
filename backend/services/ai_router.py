"""
AI Router — three-tier intelligence with graceful degradation.

Tier 1 — Gemini Pro (GEMINI_API_KEY in .env, 18-month access)
         Bulk enrichment, suburb analysis, outreach copy, report content.

Tier 2 — Claude Sonnet 4.6 (ANTHROPIC_API_KEY in .env, use sparingly)
         High-stakes outreach drafting, final copy approval, strategic reasoning.

Tier 3 — Ollama local (http://localhost:11434)
         Classification, scoring notes, quick summaries. Free. No internet.

Routing rules:
  - bulk_enrich / suburb_analysis / report_content → Gemini Pro
  - outreach_copy / call_script → Gemini Pro, fallback Claude
  - classify / score_note / quick_summary → Ollama, fallback Gemini
  - Any tier unavailable → silently falls back down the chain
  - All API calls are non-blocking; failures return a structured fallback string
  - RPM caps enforced per-tier via in-process leaky bucket to avoid rate bans

Usage:
    from services.ai_router import ask

    text = await ask(
        task="outreach_copy",
        prompt="Draft a warm intro email for...",
        lead=lead_dict,      # optional — adds address/suburb/owner context
        suburb="Bligh Park", # optional shortcut
    )
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

# ─── Model identifiers ────────────────────────────────────────────────────────

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")  # explicit version for stable rollout
CLAUDE_MODEL = "claude-sonnet-4-6"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")
GLM_BASE_URL = os.getenv("GLM_BASE_URL", "https://open.bigmodel.cn/v1")
GLM_MODEL = os.getenv("GLM_MODEL", "glm-5")
OPENCODE_BASE_URL = (
    os.getenv("OPENCODE_GO_BASE_URL")
    or os.getenv("OPENCODE_BASE_URL")
    or "https://opencode.ai/zen/go/v1"
).rstrip("/")
OPENCODE_MODEL = os.getenv("OPENCODE_MODEL", "glm-5")

# ─── Task → tier routing ─────────────────────────────────────────────────────

# Each entry is an ordered list of tiers to attempt in sequence.
_TASK_TIERS: Dict[str, List[str]] = {
    "bulk_enrich":        ["opencode", "nim", "gemini", "claude"],
    "suburb_analysis":    ["opencode", "nim", "gemini", "claude"],
    "report_content":     ["opencode", "nim", "gemini", "claude"],
    "outreach_copy":      ["opencode", "nim", "claude", "gemini"],
    "call_script":        ["opencode", "nim", "claude", "gemini"],
    "operator_brief":    ["opencode", "nim", "claude", "gemini"],
    "why_now":            ["opencode", "nim", "ollama", "gemini"],
    "classify":           ["opencode", "nim", "ollama", "gemini"],
    "score_note":         ["opencode", "nim", "ollama", "gemini"],
    "quick_summary":      ["opencode", "nim", "ollama", "gemini"],
    "mortgage_scenario":  ["opencode", "nim", "claude", "gemini"],
    # CADENCE coaching — best inference first, needs deep reasoning + long output
    "call_coaching":      ["opencode", "gemini_pro", "claude", "openai_tier", "gemini", "nim"],
    "call_analysis":      ["opencode", "gemini_pro", "claude", "gemini", "nim"],
    "default":            ["opencode", "nim", "gemini"],
}

# ─── Simple in-process rate limiter ──────────────────────────────────────────

class _TokenBucket:
    """Leaky-bucket rate limiter. Thread-safe via asyncio lock."""

    def __init__(self, rpm: int):
        self._capacity = rpm
        self._tokens = float(rpm)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            # Refill tokens proportionally
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * (self._capacity / 60.0)
            )
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False  # rate limited


_BUCKETS: Dict[str, _TokenBucket] = {
    "openai": _TokenBucket(rpm=int(os.getenv("OPENAI_RPM_CAP", "30"))),   # paid API, configurable
    "gemini": _TokenBucket(rpm=int(os.getenv("GEMINI_RPM_CAP", "12"))),   # free tier safe
    "claude": _TokenBucket(rpm=int(os.getenv("CLAUDE_RPM_CAP", "5"))),    # Tier 1 Sonnet
    "ollama": _TokenBucket(rpm=int(os.getenv("OLLAMA_RPM_CAP", "60"))),   # local, no limit
    "kimi": _TokenBucket(rpm=int(os.getenv("KIMI_RPM_CAP", "120"))),      # moonshot cap
    "qwen": _TokenBucket(rpm=int(os.getenv("QWEN_RPM_CAP", "60"))),       # nvidia nim cap
    "nim": _TokenBucket(rpm=int(os.getenv("NIM_RPM_CAP", "60"))),         # nim specific cap
    "glm": _TokenBucket(rpm=int(os.getenv("GLM_RPM_CAP", "30"))),         # GLM 5 cap
    "opencode": _TokenBucket(rpm=int(os.getenv("OPENCODE_RPM_CAP", "30"))),
}


# ─── Tier implementations ────────────────────────────────────────────────────

def _extract_gemini_text(data: Dict[str, Any]) -> Optional[str]:
    candidates = data.get("candidates", [])
    if not candidates:
        return None
    parts = candidates[0].get("content", {}).get("parts", [])
    text_parts = [part.get("text", "") for part in parts if part.get("text")]
    if not text_parts:
        return None
    return "".join(text_parts).strip() or None


def _strip_json_fences(payload: str) -> str:
    cleaned = payload.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    return cleaned.strip()


async def _call_gemini(
    prompt: str,
    system: str = "",
    *,
    model: Optional[str] = None,
    generation_config: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        return None
    if not await _BUCKETS["gemini"].acquire():
        logger.warning("[AI] Gemini rate-limited, skipping")
        return None

    full_prompt = f"{system}\n\n{prompt}" if system else prompt
    selected_model = model or GEMINI_MODEL
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{selected_model}:generateContent"
    config: Dict[str, Any] = {"maxOutputTokens": 1024, "temperature": 0.7}
    if generation_config:
        config.update(generation_config)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                headers={
                    "x-goog-api-key": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "contents": [{"parts": [{"text": full_prompt}]}],
                    "generationConfig": config,
                    "safetySettings": [
                        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    ],
                },
            )
            resp.raise_for_status()
            return _extract_gemini_text(resp.json())
    except Exception as e:
        logger.warning(f"[AI] Gemini call failed for model={selected_model}: {e}")
    return None


async def _call_gemini_json(
    prompt: str,
    schema: Dict[str, Any],
    system: str = "",
    *,
    model: Optional[str] = None,
    temperature: float = 0.1,
    max_output_tokens: int = 768,
) -> Optional[Dict[str, Any]]:
    text = await _call_gemini(
        prompt,
        system,
        model=model,
        generation_config={
            "maxOutputTokens": max_output_tokens,
            "temperature": temperature,
            "responseMimeType": "application/json",
            "responseJsonSchema": schema,
        },
    )
    if not text:
        return None
    try:
        return json.loads(_strip_json_fences(text))
    except json.JSONDecodeError as exc:
        logger.warning("[AI] Gemini JSON decode failed: %s", exc)
        return None


from anthropic import AsyncAnthropic
from anthropic import AsyncAnthropicVertex

async def _call_claude(prompt: str, system: str = "", *, model: Optional[str] = None) -> Optional[str]:
    provider = os.getenv("CLAUDE_PROVIDER", "anthropic").lower()
    
    # We check rate limits regardless of provider
    if not await _BUCKETS["claude"].acquire():
        logger.warning(f"[AI] Claude ({provider}) rate-limited, skipping")
        return None

    try:
        if provider == "vertex":
            # Requires GOOGLE_CLOUD_REGION and GOOGLE_CLOUD_PROJECT_ID
            # Assumes GOOGLE_APPLICATION_CREDENTIALS is set or gcloud auth is configured
            region = os.getenv("GOOGLE_CLOUD_REGION", "us-central1")
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
            # Vertex Claude models often have different names (e.g. claude-3-5-sonnet-v2@20241022)
            # Default to a typical Vertex model string if not explicitly set
            model_name = model or os.getenv("VERTEX_CLAUDE_MODEL", "claude-3-5-sonnet-v2@20241022")
            
            client = AsyncAnthropicVertex(region=region, project_id=project_id)
        else:
            api_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not api_key:
                logger.warning("[AI] ANTHROPIC_API_KEY not set")
                return None
            model_name = model or CLAUDE_MODEL
            client = AsyncAnthropic(api_key=api_key)

        kwargs = {
            "model": model_name,
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system

        resp = await client.messages.create(**kwargs)
        
        # Parse text from response blocks
        if resp.content and len(resp.content) > 0 and resp.content[0].type == "text":
            return resp.content[0].text.strip()
            
    except Exception as e:
        logger.warning(f"[AI] Claude ({provider}) call failed: {e}")
        
    return None


async def _call_ollama(prompt: str, system: str = "") -> Optional[str]:
    if not await _BUCKETS["ollama"].acquire():
        return None
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{OLLAMA_BASE_URL}/api/chat",
                json={"model": OLLAMA_MODEL, "messages": messages, "stream": False},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("message", {}).get("content", "").strip() or None
    except Exception as e:
        logger.debug(f"[AI] Ollama not available: {e}")
    return None


from core.utils import get_openai_api_key

async def _call_openai(
    prompt: str,
    system: str = "",
    *,
    model: Optional[str] = None,
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
) -> Dict[str, Any]:
    api_key = get_openai_api_key()
    if not api_key:
        return {"text": None, "error": "OPENAI_API_KEY not set", "usage": {}}
    if not await _BUCKETS["openai"].acquire():
        logger.warning("[AI] OpenAI rate-limited, skipping")
        return {"text": None, "error": "rate_limited", "usage": {}}

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or OPENAI_DEFAULT_MODEL

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_completion_tokens": max_output_tokens,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return {"text": text, "error": None, "usage": data.get("usage") or {}}
    except Exception as e:
        logger.warning(f"[AI] OpenAI call failed for model={selected_model}: {e}")
        return {"text": None, "error": str(e), "usage": {}}


async def _call_kimi(
    prompt: str,
    system: str = "",
    *,
    model: Optional[str] = None,
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
) -> Dict[str, Any]:
    api_key = os.getenv("KIMI_API_KEY", "")
    if not api_key:
        return {"text": None, "error": "KIMI_API_KEY not set", "usage": {}}
    if not await _BUCKETS["kimi"].acquire():
        logger.warning("[AI] Kimi rate-limited, skipping")
        return {"text": None, "error": "rate_limited", "usage": {}}

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or os.getenv("KIMI_MODEL", "moonshot-v1-32k")
    base_url = os.getenv("KIMI_BASE_URL", "https://api.moonshot.cn/v1")

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_output_tokens,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return {"text": text, "error": None, "usage": data.get("usage") or {}}
    except Exception as e:
        logger.warning(f"[AI] Kimi call failed for model={selected_model}: {e}")
        return {"text": None, "error": str(e), "usage": {}}


async def _call_qwen(
    prompt: str,
    system: str = "",
    *,
    model: Optional[str] = None,
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
) -> Dict[str, Any]:
    api_key = os.getenv("NVIDIA_API_KEY", "")
    if not api_key:
        return {"text": None, "error": "NVIDIA_API_KEY not set", "usage": {}}
    if not await _BUCKETS["qwen"].acquire():
        logger.warning("[AI] Qwen rate-limited, skipping")
        return {"text": None, "error": "rate_limited", "usage": {}}

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or "qwen/qwen3.5-397b-a17b"
    base_url = "https://integrate.api.nvidia.com/v1"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_output_tokens,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return {"text": text, "error": None, "usage": data.get("usage") or {}}
    except Exception as e:
        logger.warning(f"[AI] Qwen call failed for model={selected_model}: {e}")
        return {"text": None, "error": str(e), "usage": {}}


async def _call_nim(prompt: str, system: str = "", *, model: Optional[str] = None) -> Optional[str]:
    api_key = os.getenv("NVIDIA_API_KEY", "")
    if not api_key:
        return None
    if not await _BUCKETS["nim"].acquire():
        logger.warning("[AI] NIM rate-limited, skipping")
        return None

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or os.getenv("NVIDIA_NIM_MODEL", "meta/llama3-70b-instruct")
    base_url = os.getenv("NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": 0.2,
                    "max_tokens": 2048,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return text
    except Exception as e:
        logger.warning(f"[AI] NIM call failed for model={selected_model}: {e}")
        return None


async def _call_glm(prompt: str, system: str = "", *, model: Optional[str] = None) -> Optional[str]:
    """GLM 5 (Zhipu AI) — OpenAI-compatible API, primary provider."""
    api_key = os.getenv("GLM_API_KEY", "")
    if not api_key:
        return None
    if not await _BUCKETS["glm"].acquire():
        logger.warning("[AI] GLM rate-limited, skipping")
        return None

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or GLM_MODEL

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{GLM_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": 0.7,
                    "max_tokens": 4096,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return text
    except Exception as e:
        logger.warning(f"[AI] GLM call failed for model={selected_model}: {e}")
        return None


async def _call_opencode(prompt: str, system: str = "", *, model: Optional[str] = None) -> Optional[str]:
    api_key = (os.getenv("OPENCODE_GO_API_KEY") or os.getenv("OPENCODE_API_KEY") or "").strip()
    if not api_key:
        return None
    if not await _BUCKETS["opencode"].acquire():
        logger.warning("[AI] OpenCode rate-limited, skipping")
        return None

    messages: List[Dict[str, str]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    selected_model = model or OPENCODE_MODEL

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{OPENCODE_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "x-api-key": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "model": selected_model,
                    "messages": messages,
                    "temperature": 0.7,
                    "max_tokens": 4096,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            choice = ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            text = choice.strip() if isinstance(choice, str) else None
            return text
    except Exception as e:
        logger.warning(f"[AI] OpenCode call failed for model={selected_model}: {e}")
        return None


async def _call_gemini_pro(prompt: str, system: str = "") -> Optional[str]:
    """Gemini 2.5 Pro — best reasoning model for deep coaching analysis.
    Higher token limit, lower temperature for structured coaching output."""
    return await _call_gemini(
        prompt, system,
        model="gemini-2.5-pro",
        generation_config={
            "maxOutputTokens": 4096,
            "temperature": 0.3,
        },
    )


async def _call_openai_tier(prompt: str, system: str = "") -> Optional[str]:
    """OpenAI GPT-4o wrapper — adapts dict-returning _call_openai to Optional[str]."""
    result = await _call_openai(prompt, system, model="gpt-4o", max_output_tokens=4096)
    return result.get("text") if isinstance(result, dict) else None


_TIER_FNS = {
    "opencode": _call_opencode,
    "glm": _call_glm,
    "gemini_pro": _call_gemini_pro,
    "gemini": _call_gemini,
    "claude": _call_claude,
    "ollama": _call_ollama,
    "qwen": _call_qwen,
    "nim": _call_nim,
    "openai_tier": _call_openai_tier,
}


_CONTROL_MODEL_ALIASES: Dict[str, Dict[str, Any]] = {
    "glm_primary": {
        "provider": "opencode",
        "model_env": "OPENCODE_MODEL",
        "default_model": "glm-5",
        "fallback_chain": ["openai_planner_high"],
        "cost_band": "medium",
    },
    "openai_planner_high": {
        "provider": "qwen",
        "model_env": "CONTROL_QWEN_PLANNER_MODEL",
        "default_model": os.getenv("QWEN_PLANNER_MODEL", "qwen/qwen3.5-397b-a17b"),
        "fallback_chain": ["openai_builder_medium"],
        "cost_band": "high",
    },
    "openai_builder_medium": {
        "provider": "qwen",
        "model_env": "CONTROL_QWEN_BUILDER_MODEL",
        "default_model": os.getenv("QWEN_BUILDER_MODEL", "qwen/qwen3.5-397b-a17b"),
        "fallback_chain": ["openai_reviewer_medium"],
        "cost_band": "medium",
    },
    "openai_reviewer_medium": {
        "provider": "qwen",
        "model_env": "CONTROL_QWEN_REVIEWER_MODEL",
        "default_model": os.getenv("QWEN_REVIEWER_MODEL", "qwen/qwen3.5-397b-a17b"),
        "fallback_chain": ["gemini_delegate_small"],
        "cost_band": "medium",
    },
    "claude_writer": {
        "provider": "qwen",
        "model_env": "CONTROL_QWEN_WRITER_MODEL",
        "default_model": os.getenv("QWEN_WRITER_MODEL", "qwen/qwen3.5-397b-a17b"),
        "fallback_chain": ["gemini_delegate_small"],
        "cost_band": "medium",
    },
    "gemini_delegate_small": {
        "provider": "gemini",
        "model_env": "CONTROL_GEMINI_DELEGATE_MODEL",
        "default_model": os.getenv("GEMINI_DELEGATE_MODEL", "gemini-2.5-flash"),
        "fallback_chain": [],
        "cost_band": "low",
    },
    "nim_small": {
        "provider": "nim",
        "model_env": "CONTROL_NIM_SMALL_MODEL",
        "default_model": os.getenv("NIM_SMALL_MODEL", "meta/llama3-8b-instruct"),
        "fallback_chain": ["gemini_delegate_small"],
        "cost_band": "low",
    },
    "gemini_flash": {
        "provider": "gemini",
        "model_env": "CONTROL_GEMINI_FLASH_MODEL",
        "default_model": os.getenv("GEMINI_FLASH_MODEL", "gemini-2.5-flash"),
        "fallback_chain": ["nim_small"],
        "cost_band": "low",
    },
}



def _provider_available(provider: str) -> tuple[bool, Optional[str]]:
    if provider == "openai":
        return bool(get_openai_api_key()), "OPENAI_API_KEY not set"
    if provider == "gemini":
        return bool(os.getenv("GEMINI_API_KEY", "")), "GEMINI_API_KEY not set"
    if provider == "claude":
        claude_provider = os.getenv("CLAUDE_PROVIDER", "anthropic").lower()
        if claude_provider == "vertex":
            available = bool(os.getenv("GOOGLE_CLOUD_PROJECT_ID", ""))
            return available, "GOOGLE_CLOUD_PROJECT_ID not set"
        return bool(os.getenv("ANTHROPIC_API_KEY", "")), "ANTHROPIC_API_KEY not set"
    if provider == "ollama":
        return True, None
    if provider == "kimi":
        return bool(os.getenv("KIMI_API_KEY", "")), "KIMI_API_KEY not set"
    if provider == "glm":
        return bool(os.getenv("GLM_API_KEY", "")), "GLM_API_KEY not set"
    if provider == "opencode":
        available = bool(
            (os.getenv("OPENCODE_GO_API_KEY") or os.getenv("OPENCODE_API_KEY") or "").strip()
        )
        return available, "OPENCODE_GO_API_KEY/OPENCODE_API_KEY not set"
    if provider == "qwen":
        return bool(os.getenv("NVIDIA_API_KEY", "")), "NVIDIA_API_KEY not set"
    return False, f"unknown provider: {provider}"


def get_control_model_alias(alias: str) -> Dict[str, Any]:
    config = _CONTROL_MODEL_ALIASES.get(alias)
    if not config:
        return {
            "alias": alias,
            "provider": "unknown",
            "model": alias,
            "fallback_chain": [],
            "cost_band": "medium",
            "available": False,
            "unavailable_reason": f"unknown alias: {alias}",
        }
    model = os.getenv(config["model_env"], config["default_model"])
    available, unavailable_reason = _provider_available(config["provider"])
    return {
        "alias": alias,
        "provider": config["provider"],
        "model": model,
        "fallback_chain": list(config.get("fallback_chain") or []),
        "cost_band": config.get("cost_band", "medium"),
        "available": available,
        "unavailable_reason": None if available else unavailable_reason,
    }


def is_control_model_alias_available(alias: str) -> bool:
    return bool(get_control_model_alias(alias).get("available"))


async def run_control_model_alias(
    alias: str,
    *,
    prompt: str,
    system: str = "",
    temperature: float = 0.2,
    max_output_tokens: int = 2048,
) -> Dict[str, Any]:
    config = get_control_model_alias(alias)
    if not config.get("available"):
        return {
            "success": False,
            "provider": config.get("provider"),
            "model_alias": alias,
            "model": config.get("model"),
            "output": "",
            "usage": {},
            "error": config.get("unavailable_reason") or "provider unavailable",
            "cost_band": config.get("cost_band"),
            "fallback_chain": config.get("fallback_chain") or [],
        }

    provider = str(config.get("provider") or "")
    usage: Dict[str, Any] = {}
    error: Optional[str] = None
    output = ""
    if provider == "openai":
        result = await _call_openai(
            prompt,
            system,
            model=str(config.get("model") or OPENAI_DEFAULT_MODEL),
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )
        output = str(result.get("text") or "")
        usage = result.get("usage") or {}
        error = result.get("error")
    elif provider == "gemini":
        text = await _call_gemini(
            prompt,
            system,
            model=str(config.get("model") or GEMINI_MODEL),
            generation_config={
                "maxOutputTokens": max_output_tokens,
                "temperature": temperature,
            },
        )
        output = str(text or "")
    elif provider == "claude":
        text = await _call_claude(prompt, system, model=str(config.get("model") or CLAUDE_MODEL))
        output = str(text or "")
        if not output:
            error = "empty response"
    elif provider == "ollama":
        text = await _call_ollama(prompt, system)
        output = str(text or "")
        if not output:
            error = "empty response"
    elif provider == "kimi":
        result = await _call_kimi(
            prompt,
            system,
            model=str(config.get("model") or os.getenv("KIMI_MODEL", "moonshot-v1-32k")),
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )
        output = str(result.get("text") or "")
        usage = result.get("usage") or {}
        error = result.get("error")
    elif provider == "glm":
        text = await _call_glm(
            prompt,
            system,
            model=str(config.get("model") or GLM_MODEL),
        )
        output = str(text or "")
        if not output:
            error = "empty response"
    elif provider == "opencode":
        text = await _call_opencode(
            prompt,
            system,
            model=str(config.get("model") or OPENCODE_MODEL),
        )
        output = str(text or "")
        if not output:
            error = "empty response"
    elif provider == "qwen":
        result = await _call_qwen(
            prompt,
            system,
            model=str(config.get("model") or "qwen/qwen3.5-397b-a17b"),
            temperature=temperature,
            max_output_tokens=max_output_tokens,
        )
        output = str(result.get("text") or "")
        usage = result.get("usage") or {}
        error = result.get("error")
    else:
        error = f"unsupported provider: {provider}"

    return {
        "success": bool(output),
        "provider": provider,
        "model_alias": alias,
        "model": config.get("model"),
        "output": output,
        "usage": usage,
        "error": error or (None if output else "empty response"),
        "cost_band": config.get("cost_band"),
        "fallback_chain": config.get("fallback_chain") or [],
    }


# ─── Context builder ─────────────────────────────────────────────────────────

def _build_system(task: str, lead: Optional[Dict[str, Any]], suburb: Optional[str]) -> str:
    """Build a consistent system prompt for property outreach tasks."""
    parts = [
        "You are an AI assistant for Laing+Simmons Oakville | Windsor, "
        "a real estate agency in Western Sydney. "
        "The operator is Nitin Puri. "
        "Be concise, factual, and professional. "
        "Never invent property data. "
        "Output plain text unless HTML is explicitly requested.",
    ]
    if lead:
        name = lead.get("owner_name") or "the owner"
        addr = lead.get("address") or ""
        sub = lead.get("suburb") or suburb or ""
        archetype = lead.get("lead_archetype") or ""
        if addr:
            parts.append(f"Property: {addr}.")
        if name:
            parts.append(f"Owner: {name}.")
        if sub:
            parts.append(f"Suburb: {sub}.")
        if archetype:
            parts.append(f"Lead type: {archetype}.")
    elif suburb:
        parts.append(f"Target suburb: {suburb}.")
    return " ".join(parts)


# ─── Public API ───────────────────────────────────────────────────────────────

async def ask(
    task: str,
    prompt: str,
    lead: Optional[Dict[str, Any]] = None,
    suburb: Optional[str] = None,
    system_override: Optional[str] = None,
) -> str:
    """
    Route a prompt to the best available AI tier for the given task.

    Returns the model's response as a string.
    If all tiers fail or are unavailable, returns an empty string —
    callers must handle the empty-string case gracefully.

    Args:
        task: One of the keys in _TASK_TIERS (e.g. "outreach_copy", "classify").
              Unknown tasks fall back to "default" routing.
        prompt: The user-facing prompt to send to the model.
        lead: Optional lead dict — adds property context to the system prompt.
        suburb: Optional suburb name shortcut for suburb-level tasks.
        system_override: If provided, replaces the auto-generated system prompt.
    """
    tiers = _TASK_TIERS.get(task, _TASK_TIERS["default"])
    system = system_override or _build_system(task, lead, suburb)

    for tier in tiers:
        fn = _TIER_FNS.get(tier)
        if fn is None:
            continue
        try:
            result = await fn(prompt, system)
            if result:
                logger.debug(f"[AI] task={task} tier={tier} chars={len(result)}")
                return result
        except Exception as e:
            logger.warning(f"[AI] tier={tier} task={task} exception: {e}")

    logger.warning(f"[AI] All tiers exhausted for task={task}")
    return ""


async def classify_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Quick classification: returns archetype guess and a one-sentence 'why now'.

    Output example:
        {"archetype": "mortgage_cliff", "why_now": "Fixed rate expires ~Sep 2026"}

    Falls back to current lead values if AI unavailable.
    """
    owner = lead.get("owner_name") or "Owner"
    address = lead.get("address") or "unknown address"
    settlement = lead.get("settlement_date") or "unknown"
    archetype = lead.get("lead_archetype") or "default"

    prompt = (
        f"Classify this property lead and give a one-sentence 'why now' urgency note.\n"
        f"Owner: {owner}\n"
        f"Address: {address}\n"
        f"Settlement date: {settlement}\n"
        f"Current archetype: {archetype}\n\n"
        f"Respond with JSON only: "
        f'{{\"archetype\": \"...\", \"why_now\": \"...\"}}'
    )
    result = await ask("classify", prompt, lead=lead)
    if not result:
        return {"archetype": archetype, "why_now": ""}
    try:
        # Strip markdown code fences if present
        clean = result.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        return json.loads(clean)
    except Exception:
        return {"archetype": archetype, "why_now": result[:200]}


async def draft_outreach_email(
    lead: Dict[str, Any],
    archetype: Optional[str] = None,
    brand: str = "ls",
) -> Dict[str, str]:
    """
    Draft a personalized outreach email subject + body for a lead.

    Returns {"subject": "...", "body": "..."}
    Falls back to the static template from email_templates.py if AI unavailable.
    """
    from services.email_templates import render_template

    arch = archetype or lead.get("lead_archetype") or "default"
    first = (lead.get("owner_name") or "there").split()[0]
    address = lead.get("address") or "your property"
    suburb = lead.get("suburb") or "the area"

    brand_sig = "Nitin Puri, Laing+Simmons Oakville | Windsor (0430 042 041)" if brand == "ls" \
        else "Shahid, Ownit1st Loans (04 85 85 7881)"

    prompt = (
        f"Draft a short, warm outreach email (3–4 paragraphs max) for the following property lead.\n"
        f"Archetype: {arch}\n"
        f"First name: {first}\n"
        f"Property: {address}, {suburb}\n"
        f"Brand: {brand_sig}\n\n"
        f"Rules:\n"
        f"- Do not invent specific sale prices or dates unless provided\n"
        f"- Be conversational, not pushy\n"
        f"- End with a soft call-to-action (brief call this week)\n"
        f"- Output plain text (no HTML)\n\n"
        f"Format:\n"
        f"SUBJECT: <subject line>\n"
        f"BODY:\n<email body>"
    )

    result = await ask("outreach_copy", prompt, lead=lead)
    if result and "SUBJECT:" in result:
        lines = result.strip().split("\n")
        subject = ""
        body_lines = []
        found_subject = False
        in_body = False
        for line in lines:
            if line.startswith("SUBJECT:"):
                subject = line.replace("SUBJECT:", "").strip()
                found_subject = True
            elif line.startswith("BODY:"):
                in_body = True
            elif found_subject and not in_body and line.strip():
                # No BODY: marker — everything non-empty after subject is the body
                in_body = True
                body_lines.append(line)
            elif in_body:
                body_lines.append(line)
        if subject and body_lines:
            return {"subject": subject, "body": "\n".join(body_lines).strip()}

    # Fallback to static template
    return render_template(arch, lead, brand=brand)


async def generate_suburb_snapshot(suburb: str, data: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a short suburb market narrative for use in PDFs or emails.
    data: optional dict with recent_sales, median_price, days_on_market, etc.
    Returns a markdown string (3–5 sentences).
    """
    context = ""
    if data:
        context = "\n".join(f"- {k}: {v}" for k, v in data.items() if v)

    prompt = (
        f"Write a 3–5 sentence property market snapshot for {suburb}, NSW, Australia.\n"
        f"Tone: professional, data-driven, factual.\n"
        f"Do not invent specific numbers unless provided below.\n"
        f"Do not use marketing clichés.\n"
    )
    if context:
        prompt += f"\nAvailable data:\n{context}\n"
    prompt += "\nOutput plain text only."

    return await ask("suburb_analysis", prompt, suburb=suburb) or \
        f"{suburb} property market data is being compiled."


async def score_note(lead: Dict[str, Any]) -> str:
    """
    Generate a one-sentence explanation of why this lead scored as it did.
    Used in the lead detail panel and call scripts.
    """
    address = lead.get("address") or "this property"
    heat = lead.get("heat_score") or 0
    archetype = lead.get("lead_archetype") or "default"
    settlement = lead.get("settlement_date") or ""

    prompt = (
        f"In one concise sentence, explain why this property lead has a heat score of {heat}/100.\n"
        f"Address: {address}\n"
        f"Lead type: {archetype}\n"
        f"Settlement date: {settlement or 'unknown'}\n"
        f"Keep it factual and useful for the operator."
    )
    return await ask("score_note", prompt, lead=lead) or ""
