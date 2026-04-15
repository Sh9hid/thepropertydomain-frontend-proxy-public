from __future__ import annotations

from hermes.workers.content_worker import generate_content_drafts


async def repurpose_item(findings, provider, trace):
    trace.append({"stage": "repurpose_start", "count": len(findings)})
    return await generate_content_drafts(findings, provider, trace)
