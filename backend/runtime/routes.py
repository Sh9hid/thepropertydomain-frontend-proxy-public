from __future__ import annotations

from fastapi import FastAPI

from api.routes import admin, analytics, auth, campaigns, coach, communications, control, conversations
from api.routes import cotality, deals, departments, dialing, distress, docs as docs_router
from api.routes import documents as documents_router
from api.routes import enrichment, evidence, forms, funnels, growth, ingest, leads, listings, memory
from api.routes import missed_deals, mortgage, operator, opportunities, orchestration, outreach
from api.routes import outreach_queue
from api.routes import property_intel, qwen, rea, recordings, research, review, signals, signing, speech
from api.routes import system, tasks, tickets, underwriter, voice_training, waitlist
from hermes.routes import router as hermes_router
from api.routes import rea_agent

try:
    from api.routes import templates as templates_router
    from api.routes import tracking as tracking_router
    from api.routes import chat as chat_router
    from api.routes import developers as developers_router
except Exception:
    templates_router = tracking_router = chat_router = developers_router = None


def register_routers(app: FastAPI) -> None:
    for router in (
        auth.router,
        leads.router,
        tasks.router,
        operator.router,
        analytics.router,
        communications.router,
        outreach.router,
        system.router,
        cotality.router,
        deals.router,
        ingest.router,
        recordings.router,
        enrichment.router,
        review.router,
        forms.router,
        signing.router,
        listings.router,
        rea.router,
        rea_agent.router,
        evidence.router,
        control.router,
        funnels.router,
        property_intel.router,
        distress.router,
        docs_router.router,
        documents_router.router,
        qwen.router,
        speech.router,
        orchestration.router,
        tickets.router,
        conversations.router,
        research.router,
        voice_training.router,
        memory.router,
        waitlist.router,
        opportunities.router,
        missed_deals.router,
        coach.router,
        signals.router,
        admin.router,
        growth.router,
        campaigns.router,
        mortgage.router,
        hermes_router,
        underwriter.router,
        departments.router,
        dialing.router,
        outreach_queue.router,
    ):
        app.include_router(router)
    for optional_router in (templates_router, tracking_router, chat_router, developers_router):
        if optional_router and hasattr(optional_router, "router"):
            app.include_router(optional_router.router)
