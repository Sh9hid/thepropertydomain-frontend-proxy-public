from __future__ import annotations

from typing import List

from hermes.schemas import CampaignDraftPayload


_CAMPAIGN_GOALS = {
    "app_sales": "Book discovery calls for the product without fabricated claims.",
    "seller": "Move prospective sellers from interest to reply and booking.",
    "buyer": "Nurture buyers with grounded local commentary and next-step clarity.",
    "mortgage": "Advance refinance or mortgage leads toward a qualified conversation.",
}


async def build_campaign_drafts(campaign_type: str, prompt: str, provider, trace, channel: str = "email") -> List[CampaignDraftPayload]:
    goal = _CAMPAIGN_GOALS.get(campaign_type, "Drive a grounded next step.")
    angle = await provider.rewrite(
        prompt or goal,
        instruction="Convert this into a concise outreach angle without hype or fabricated claims.",
    )

    stages = [
        ("first_touch", "Quick idea for you"),
        ("follow_up_1", "Following up on the workflow angle"),
        ("follow_up_2", "One practical next step"),
        ("reengage", "Worth revisiting?"),
    ]

    drafts: List[CampaignDraftPayload] = []
    for stage, subject in stages:
        message = (
            f"{angle}\n\n"
            f"Goal: {goal}\n"
            "No inflated metrics, no fake urgency, and no silent sending. "
            "This is a draft prepared for operator review."
        )
        drafts.append(
            CampaignDraftPayload(
                campaign_type=campaign_type,  # type: ignore[arg-type]
                stage=stage,  # type: ignore[arg-type]
                channel=channel,  # type: ignore[arg-type]
                subject=subject,
                message=message,
                goal=goal,
            )
        )
    trace.append({"stage": "campaign_generate", "campaign_type": campaign_type, "count": len(drafts)})
    return drafts
