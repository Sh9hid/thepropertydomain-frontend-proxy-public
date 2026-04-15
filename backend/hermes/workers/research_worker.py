from __future__ import annotations

from typing import Dict, List

from hermes.schemas import ResearchFindingPayload
from hermes.scoring import (
    extract_tags,
    infer_company_scope,
    infer_signal_type,
    map_proposed_actions,
    score_actionability,
    score_confidence,
    score_novelty,
)
from hermes.sources import blogs, reddit, repos, rss, websites, x_signals


_COLLECTORS = {
    "rss": rss.collect,
    "blog": blogs.collect,
    "official_doc": websites.collect,
    "website": websites.collect,
    "repo": repos.collect,
    "reddit": reddit.collect,
    "x": x_signals.collect,
}


def _why_it_matters(topic: str, signal_type: str, company_scope: str) -> str:
    scope_copy = {
        "app_sales": "app growth, product positioning, and outbound app sales",
        "real_estate": "seller acquisition, buyer nurture, and agent workflows",
        "mortgage": "mortgage/refinance lead development and broker nurture",
        "shared": "shared product, outreach, and content decisions",
    }
    return (
        f"This {signal_type} signal changes how we should think about {scope_copy.get(company_scope, 'execution')}."
        f" It is worth capturing because it can be translated into a concrete workflow, message, or operator decision."
    )


async def collect_findings_for_source(source, provider, trace: List[Dict]) -> List[ResearchFindingPayload]:
    collector = _COLLECTORS.get(source.source_type, websites.collect)
    raw_items = await collector(source)
    findings: List[ResearchFindingPayload] = []
    seen_urls = set()

    trace.append(
        {
            "stage": "research_fetch",
            "source_id": source.id,
            "source_name": source.name,
            "source_type": source.source_type,
            "items_seen": len(raw_items),
        }
    )

    for item in raw_items:
        url = str(item.get("url") or "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        topic = str(item.get("title") or "").strip()
        summary = str(item.get("summary") or "").strip()
        if summary:
            summary = await provider.summarize(summary, context=topic)
        else:
            summary = topic
        text_blob = " ".join([topic, summary, source.name, " ".join(source.tags_json or [])]).strip()
        signal_type = infer_signal_type(source.source_type, text_blob)
        company_scope = infer_company_scope(source.company_scope, text_blob)
        payload = ResearchFindingPayload(
            source_type=source.source_type,
            source_name=source.name,
            url=url,
            topic=topic,
            summary=summary,
            why_it_matters=_why_it_matters(topic, signal_type, company_scope),
            company_scope=company_scope,
            signal_type=signal_type,
            novelty_score=score_novelty(text_blob, str(item.get("published_at") or "").strip(), source.source_type),
            confidence_score=score_confidence(summary, float(source.credibility_score or 0.7), source.source_type),
            actionability_score=score_actionability(text_blob, signal_type),
            proposed_actions=map_proposed_actions(signal_type, company_scope, text_blob),
        )
        findings.append(payload)
        trace.append(
            {
                "stage": "research_extract",
                "source_name": source.name,
                "topic": payload.topic,
                "signal_type": payload.signal_type,
                "tags": extract_tags(payload.topic, payload.summary),
            }
        )
    return findings
