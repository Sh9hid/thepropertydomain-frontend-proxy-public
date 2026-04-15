from __future__ import annotations

from typing import List

from hermes.schemas import ContentDraftPayload


def _short_body(finding) -> str:
    return (
        f"Signal: {finding.summary}\n\n"
        f"Why it matters: {finding.why_it_matters}\n\n"
        "Takeaway: translate the underlying workflow into something operators can use natively."
    )


def _proposed_actions(finding) -> List[str]:
    actions = getattr(finding, "proposed_actions_json", None)
    if actions:
        return list(actions)
    return []


async def generate_content_drafts(findings, provider, trace) -> List[ContentDraftPayload]:
    drafts: List[ContentDraftPayload] = []
    for finding in findings:
        base_hook = f"{finding.topic}: the useful part is the workflow behind it, not the headline."
        hook = await provider.rewrite(base_hook, instruction="Tighten this hook for native social consumption.")
        common_cta = "Reply if you want this turned into a usable internal playbook."
        source_refs = [finding.source_url]

        drafts.extend(
            [
                ContentDraftPayload(
                    content_type="x_post",
                    target_audience="founders" if finding.company_scope == "app_sales" else "agents",
                    hook=hook,
                    body=_short_body(finding),
                    cta=common_cta,
                    source_refs=source_refs,
                    repurposable=True,
                ),
                ContentDraftPayload(
                    content_type="linkedin",
                    target_audience="proptech" if finding.company_scope == "app_sales" else "principals",
                    hook=f"What this signal means for the next quarter: {finding.topic}",
                    body=(
                        f"{finding.summary}\n\n"
                        f"{finding.why_it_matters}\n\n"
                        "The operator move is to convert this into repeatable process, not generic commentary."
                    ),
                    cta="Use this as a briefing note before your next strategy review.",
                    source_refs=source_refs,
                    repurposable=True,
                ),
                ContentDraftPayload(
                    content_type="newsletter",
                    target_audience="mortgage" if finding.company_scope == "mortgage" else "agents",
                    hook=f"One market signal worth stealing this week: {finding.topic}",
                    body=(
                        f"Observed signal: {finding.summary}\n\n"
                        f"Why it matters now: {finding.why_it_matters}\n\n"
                        f"Suggested uses: {', '.join(_proposed_actions(finding))}"
                    ),
                    cta="Queue this for the next operator digest if it fits the brand.",
                    source_refs=source_refs,
                    repurposable=True,
                ),
            ]
        )
        trace.append(
            {
                "stage": "content_generate",
                "finding_id": finding.id,
                "topic": finding.topic,
                "draft_count": 3,
            }
        )
    return drafts
