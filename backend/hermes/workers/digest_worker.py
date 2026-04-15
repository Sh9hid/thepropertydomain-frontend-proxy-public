from __future__ import annotations

from typing import Dict, Iterable, List

from hermes.scoring import sort_findings


def _finding_card(finding) -> Dict:
    return {
        "id": finding.id,
        "topic": finding.topic,
        "summary": finding.summary,
        "why_it_matters": finding.why_it_matters,
        "source_name": finding.source_name,
        "source_url": finding.source_url,
        "company_scope": finding.company_scope,
        "signal_type": finding.signal_type,
        "actionability_score": finding.actionability_score,
    }


def _content_card(content) -> Dict:
    return {
        "id": content.id,
        "content_type": content.content_type,
        "audience": content.audience,
        "hook": content.hook,
        "status": content.status,
    }


def _campaign_card(campaign) -> Dict:
    return {
        "id": campaign.id,
        "campaign_type": campaign.campaign_type,
        "channel": campaign.channel,
        "stage": campaign.stage,
        "subject": campaign.subject,
        "status": campaign.status,
    }


def build_digest(findings: Iterable, content: Iterable, campaigns: Iterable) -> Dict:
    finding_list = sort_findings(findings)
    content_list = list(content)
    campaign_list = list(campaigns)

    risks = [
        _finding_card(item)
        for item in finding_list
        if any(token in f"{item.topic} {item.summary}".lower() for token in ("risk", "competitor", "changelog", "release", "changed"))
    ][:10]

    recommended_actions: List[str] = []
    for item in finding_list:
        recommended_actions.extend(list(getattr(item, "proposed_actions_json", []) or []))
    deduped_actions: List[str] = []
    seen = set()
    for action in recommended_actions:
        key = action.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped_actions.append(action)
        if len(deduped_actions) >= 10:
            break

    return {
        "top_new_opportunities": [_finding_card(item) for item in finding_list[:10]],
        "top_research_insights": [_finding_card(item) for item in finding_list[:10]],
        "top_content_opportunities": [_content_card(item) for item in content_list[:10]],
        "top_risks_or_competitor_moves": risks,
        "top_recommended_actions": deduped_actions,
        "top_campaigns": [_campaign_card(item) for item in campaign_list[:10]],
    }
