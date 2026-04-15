from __future__ import annotations

import hashlib
import re
from typing import Iterable, List
from urllib.parse import urlsplit, urlunsplit

from .jobs import ACTION_BUCKETS, compact_list


_TAG_RE = re.compile(r"[^a-z0-9]+")


def bounded_score(value: float, *, low: float = 0.0, high: float = 1.0) -> float:
    return round(max(low, min(high, value)), 4)


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    parts = urlsplit(url)
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def slugify(value: str) -> str:
    return _TAG_RE.sub("-", normalize_text(value).lower()).strip("-")


def build_dedupe_key(url: str, title: str, published_at: str | None) -> str:
    normalized = "||".join(
        [
            normalize_url(url),
            normalize_text(title).lower(),
            (published_at or "").strip(),
        ]
    )
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def extract_tags(*values: str) -> List[str]:
    tags: List[str] = []
    for value in values:
        for raw_token in re.findall(r"[A-Za-z0-9_+#-]{4,}", value or ""):
            token = slugify(raw_token)
            if token and token not in tags:
                tags.append(token)
    return tags[:12]


def infer_signal_type(source_type: str, text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in ("launch", "release", "changelog", "feature", "shipped")):
        return "product"
    if any(token in lowered for token in ("automation", "workflow", "orchestration", "agent", "sequence")):
        return "automation"
    if any(token in lowered for token in ("pricing", "positioning", "acquisition", "nurture", "outreach")):
        return "gtm"
    if any(token in lowered for token in ("dataset", "market", "rates", "inventory", "median", "volume")):
        return "market" if "market" in lowered or "rates" in lowered else "data"
    if any(token in lowered for token in ("post", "thread", "newsletter", "content")):
        return "content"
    if source_type == "repo":
        return "automation"
    return "ops"


def infer_company_scope(default_scope: str, text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in ("mortgage", "broker", "refinance", "refi", "home loan")):
        return "mortgage"
    if any(token in lowered for token in ("crm", "proptech", "saas", "app", "workspace", "operator ui")):
        return "app_sales"
    if any(token in lowered for token in ("listing", "seller", "buyer", "real estate", "agent")):
        return "real_estate"
    return default_scope or "shared"


def score_novelty(text: str, published_at: str | None, source_type: str) -> float:
    lowered = (text or "").lower()
    score = 0.45
    if published_at:
        score += 0.1
    if source_type in {"repo", "official_doc", "x"}:
        score += 0.08
    if any(token in lowered for token in ("new", "launch", "release", "shipped", "added", "changed")):
        score += 0.18
    if any(token in lowered for token in ("competitor", "workflow", "automation", "seller", "mortgage", "lead gen")):
        score += 0.1
    return bounded_score(score)


def score_confidence(summary: str, credibility_score: float, source_type: str) -> float:
    score = 0.35 + max(0.0, min(1.0, credibility_score)) * 0.45
    if len(normalize_text(summary)) >= 80:
        score += 0.1
    if source_type in {"official_doc", "blog", "rss", "repo"}:
        score += 0.05
    return bounded_score(score)


def score_actionability(text: str, signal_type: str) -> float:
    lowered = (text or "").lower()
    score = 0.42
    if signal_type in {"automation", "gtm", "product", "content"}:
        score += 0.18
    if any(token in lowered for token in ("sequence", "template", "playbook", "copy", "workflow", "prompt")):
        score += 0.18
    if any(token in lowered for token in ("seller", "buyer", "mortgage", "whatsapp", "email", "lead")):
        score += 0.12
    return bounded_score(score)


def map_proposed_actions(signal_type: str, company_scope: str, text: str) -> List[str]:
    lowered = (text or "").lower()
    actions: List[str] = []
    if signal_type in {"product", "automation", "ops"} or "repo" in lowered:
        actions.append("build in app")
    if signal_type in {"gtm", "automation"} or any(token in lowered for token in ("nurture", "follow-up", "outreach", "mortgage")):
        actions.append("use in outreach")
    if signal_type in {"content", "market", "gtm"} or any(token in lowered for token in ("post", "newsletter", "thread", "commentary")):
        actions.append("use in content")
    if company_scope == "shared":
        actions.append("save for later")
    if not actions:
        actions.append("save for later")
    return compact_list(actions + ACTION_BUCKETS, limit=4)


def sort_findings(findings: Iterable[object]) -> List[object]:
    return sorted(
        findings,
        key=lambda item: (
            getattr(item, "actionability_score", 0.0),
            getattr(item, "novelty_score", 0.0),
            getattr(item, "created_at", ""),
        ),
        reverse=True,
    )
