from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from typing import Any, Dict, List, Optional

import httpx


log = logging.getLogger(__name__)


def _api_key() -> str:
    return os.getenv("NVIDIA_API_KEY", "").strip() or os.getenv("NGC_API_KEY", "").strip()


def _safe_json_loads(value: str) -> Dict[str, Any]:
    cleaned = (value or "").strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    cleaned = cleaned.strip()
    if not cleaned:
        return {}
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


class LLMProvider:
    async def summarize(self, text: str, *, context: str = "") -> str:
        raise NotImplementedError

    async def classify(self, text: str, *, labels: List[str]) -> str:
        raise NotImplementedError

    async def extract_structured(self, text: str, *, schema: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    async def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError

    async def rewrite(self, text: str, *, instruction: str) -> str:
        raise NotImplementedError


class DummyProvider(LLMProvider):
    async def summarize(self, text: str, *, context: str = "") -> str:
        base = " ".join(part for part in [context.strip(), text.strip()] if part).strip()
        if len(base) <= 220:
            return base
        return f"{base[:217].rstrip()}..."

    async def classify(self, text: str, *, labels: List[str]) -> str:
        lowered = (text or "").lower()
        for label in labels:
            if label.lower() in lowered:
                return label
        return labels[0] if labels else "unknown"

    async def extract_structured(self, text: str, *, schema: Dict[str, Any]) -> Dict[str, Any]:
        return {}

    async def embed(self, texts: List[str]) -> List[List[float]]:
        vectors: List[List[float]] = []
        for text in texts:
            digest = hashlib.sha1((text or "").encode("utf-8")).digest()
            vectors.append([round(byte / 255.0, 6) for byte in digest[:8]])
        return vectors

    async def rewrite(self, text: str, *, instruction: str) -> str:
        return text.strip()


class NvidiaNIMProvider(LLMProvider):
    def __init__(self) -> None:
        self.api_key = _api_key()
        self.base_url = os.getenv("NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1").rstrip("/")
        self.summary_model = os.getenv("HERMES_NVIDIA_SUMMARY_MODEL", os.getenv("NVIDIA_NIM_MODEL", "meta/llama-3.1-70b-instruct"))
        self.rewrite_model = os.getenv("HERMES_NVIDIA_REWRITE_MODEL", self.summary_model)
        self.extract_model = os.getenv("HERMES_NVIDIA_EXTRACT_MODEL", self.summary_model)
        self.classify_model = os.getenv("HERMES_NVIDIA_CLASSIFY_MODEL", self.summary_model)
        self.embed_model = os.getenv("HERMES_NVIDIA_EMBED_MODEL", os.getenv("NVIDIA_NIM_EMBED_MODEL", "nvidia/nv-embedqa-e5-v5"))

    async def _chat(self, *, model: str, system: str, user: str, temperature: float = 0.1, max_tokens: int = 700) -> Optional[str]:
        if not self.api_key:
            return None
        try:
            async with httpx.AsyncClient(timeout=35.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "temperature": temperature,
                        "max_tokens": max_tokens,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                    },
                )
                response.raise_for_status()
                data = response.json()
                choices = data.get("choices") or []
                if not choices:
                    return None
                message = choices[0].get("message") or {}
                return str(message.get("content") or "").strip() or None
        except Exception as exc:
            log.warning("Hermes NVIDIA NIM chat failure model=%s error=%s", model, exc)
            return None

    async def summarize(self, text: str, *, context: str = "") -> str:
        prompt = f"Context: {context}\n\nText:\n{text}\n\nReturn one concise factual summary under 90 words."
        result = await self._chat(
            model=self.summary_model,
            system="You summarize public source material without inventing facts.",
            user=prompt,
            temperature=0.0,
            max_tokens=180,
        )
        return result or await DummyProvider().summarize(text, context=context)

    async def classify(self, text: str, *, labels: List[str]) -> str:
        if not labels:
            return "unknown"
        result = await self._chat(
            model=self.classify_model,
            system="Classify the text into exactly one of the provided labels. Return the label only.",
            user=f"Labels: {labels}\n\nText:\n{text}",
            temperature=0.0,
            max_tokens=20,
        )
        if result and result.strip() in labels:
            return result.strip()
        return await DummyProvider().classify(text, labels=labels)

    async def extract_structured(self, text: str, *, schema: Dict[str, Any]) -> Dict[str, Any]:
        result = await self._chat(
            model=self.extract_model,
            system="Extract only grounded fields from the text and return valid JSON. Omit fields you cannot support.",
            user=f"Schema:\n{json.dumps(schema, indent=2)}\n\nText:\n{text}",
            temperature=0.0,
            max_tokens=400,
        )
        return _safe_json_loads(result or "")

    async def embed(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        if not self.api_key:
            return await DummyProvider().embed(texts)
        try:
            async with httpx.AsyncClient(timeout=35.0) as client:
                response = await client.post(
                    f"{self.base_url}/embeddings",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={"model": self.embed_model, "input": texts},
                )
                response.raise_for_status()
                data = response.json()
                vectors = [item.get("embedding") for item in data.get("data") or [] if item.get("embedding")]
                if vectors:
                    return vectors
        except Exception as exc:
            log.warning("Hermes NVIDIA NIM embed failure model=%s error=%s", self.embed_model, exc)
        return await DummyProvider().embed(texts)

    async def rewrite(self, text: str, *, instruction: str) -> str:
        result = await self._chat(
            model=self.rewrite_model,
            system="Rewrite the text while preserving every grounded fact and avoiding embellishment.",
            user=f"Instruction: {instruction}\n\nText:\n{text}",
            temperature=0.15,
            max_tokens=max(120, min(500, math.ceil(len(text) / 2))),
        )
        return result or text.strip()


def build_llm_provider() -> LLMProvider:
    provider_name = (os.getenv("HERMES_LLM_PROVIDER", "dummy") or "dummy").strip().lower()
    if provider_name == "nvidia_nim" and _api_key():
        return NvidiaNIMProvider()
    return DummyProvider()


def build_embed_provider() -> LLMProvider:
    provider_name = (os.getenv("HERMES_EMBED_PROVIDER", os.getenv("HERMES_LLM_PROVIDER", "dummy")) or "dummy").strip().lower()
    if provider_name == "nvidia_nim" and _api_key():
        return NvidiaNIMProvider()
    return DummyProvider()
