from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any, Optional
from jinja2 import Template
import os

from core.database import get_session
from core.config import BRAND_NAME, BRAND_LOGO_URL
from services.listing_workflow import (
    FTR32_GUIDE_URL,
    build_signing_context,
    complete_signing_session,
    mark_signing_session_viewed,
)

try:
    from services.velvet_engine import velvet_engine
    VELVET_IMPORT_ERROR: Optional[Exception] = None
except Exception as exc:  # pragma: no cover - environment-specific native dependency
    velvet_engine = None
    VELVET_IMPORT_ERROR = exc

router = APIRouter()

# --- THE SIGNING ROOM (HTML VIEW) ---
# This is what the client sees when they click the email link.
# Built with HTMX for high-speed, frictionless interactions.

SIGNING_ROOM_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Signing Room | {{ brand_name }}</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .glass { background: rgba(255, 255, 255, 0.7); backdrop-filter: blur(10px); border: 1px solid rgba(255, 255, 255, 0.3); }
        .accent-red { color: #E21937; }
        .bg-accent-red { background-color: #E21937; }
    </style>
</head>
<body class="bg-gray-50 text-gray-900 font-sans min-h-screen flex flex-col items-center p-4">
    <header class="w-full max-w-2xl flex justify-between items-center py-8">
        <img src="{{ logo_url }}" alt="{{ brand_name }}" class="h-12">
        <div class="text-xs font-bold uppercase tracking-widest text-gray-400">Secure Signing Room</div>
    </header>

    <main id="main-content" class="w-full max-w-2xl glass rounded-3xl p-8 shadow-2xl">
        <div class="mb-8">
            <h1 class="text-3xl font-bold mb-2">Agency Agreement</h1>
            <p class="text-gray-500">Please review the details for <strong>{{ property_address }}</strong></p>
        </div>

        <section class="space-y-6">
            <div class="p-4 bg-white/50 rounded-xl border border-gray-100">
                <h2 class="text-xs font-bold uppercase text-gray-400 mb-2">Principal (Vendors)</h2>
                {% for vendor in vendors %}
                <div class="text-lg font-medium">{{ vendor }}</div>
                {% endfor %}
            </div>

            <div class="grid grid-cols-2 gap-4">
                <div class="p-4 bg-white/50 rounded-xl border border-gray-100">
                    <h2 class="text-xs font-bold uppercase text-gray-400 mb-2">Commission</h2>
                    <div class="text-lg font-medium accent-red">{{ commission_pct }}%</div>
                    <div class="text-xs text-gray-500">Approx ${{ commission_val }}</div>
                </div>
                <div class="p-4 bg-white/50 rounded-xl border border-gray-100">
                    <h2 class="text-xs font-bold uppercase text-gray-400 mb-2">Campaign Type</h2>
                    <div class="text-lg font-medium">{{ campaign_type }}</div>
                </div>
            </div>

            <div class="p-4 bg-blue-50 rounded-xl border border-blue-100 text-sm text-blue-800">
                <p><strong>Required Disclosure:</strong> By signing, you acknowledge receipt of the <a href="{{ fair_trading_guide }}" target="_blank" class="underline font-bold">NSW Fair Trading Guide (FTR32)</a>.</p>
            </div>

            {% if authority_html %}
            <div class="mt-10 rounded-2xl bg-white/70 border border-gray-100 p-5 text-sm text-gray-700 overflow-x-auto">
                {{ authority_html | safe }}
            </div>
            {% endif %}

            <form
                id="signing-form"
                class="mt-12 pt-8 border-t border-gray-100"
                hx-post="/api/signing/{{ lead_id }}/execute"
                hx-target="#main-content"
                hx-swap="innerHTML"
            >
                <div class="grid grid-cols-1 gap-4 mb-6">
                    <input type="hidden" name="session_token" value="{{ session_token or '' }}">
                    <input type="text" name="signer_name" value="{{ signer_name or '' }}" placeholder="Your full name" class="w-full rounded-xl border border-gray-200 px-4 py-3">
                    <input type="email" name="signer_email" value="{{ signer_email or '' }}" placeholder="Your email address" class="w-full rounded-xl border border-gray-200 px-4 py-3">
                </div>
                <div class="flex items-start gap-3 mb-6">
                    <input type="checkbox" id="consent" name="consent" class="mt-1 h-5 w-5 rounded border-gray-300 text-red-600 focus:ring-red-500">
                    <label for="consent" class="text-sm text-gray-600">
                        I consent to signing this agreement electronically in accordance with the <em>NSW Electronic Transactions Act 2000</em>.
                    </label>
                </div>

                <button
                    type="submit"
                    class="w-full py-4 bg-accent-red text-white font-bold rounded-2xl shadow-lg hover:opacity-90 transition-all transform active:scale-95"
                >
                    Confirm & Sign Agreement
                </button>
            </form>
        </section>
    </main>

    <footer class="mt-12 text-center text-xs text-gray-400">
        &copy; 2026 {{ brand_name }} | Encrypted & Legally Compliant (NSW PSA Act 2002)
    </footer>
</body>
</html>
"""

@router.get("/signing/{lead_id}", response_class=HTMLResponse)
async def get_signing_room(lead_id: str, session_token: Optional[str] = Query(default=None, alias="session"), session: AsyncSession = Depends(get_session)):
    """
    Returns the high-fidelity Signing Room HTML.
    Clients are redirected here from their email.
    """
    res = await session.execute(text("SELECT * FROM leads WHERE id = :id"), {"id": lead_id})
    lead = res.mappings().first()
    if not lead:
        raise HTTPException(status_code=404, detail="Agreement link invalid or expired")
    
    if session_token:
        try:
            signing_context = await build_signing_context(session, lead_id, session_token)
            await mark_signing_session_viewed(session, lead_id, session_token)
            context = {
                "brand_name": BRAND_NAME,
                "logo_url": BRAND_LOGO_URL,
                "lead_id": lead_id,
                "property_address": lead.get("address"),
                "vendors": [lead.get("owner_name") or "Vendor"],
                "commission_pct": "Approved",
                "commission_val": "Workflow pack",
                "campaign_type": (signing_context["workflow_payload"]["workflow"].get("authority_type") or "exclusive").upper(),
                "fair_trading_guide": FTR32_GUIDE_URL,
                "authority_html": signing_context["authority_html"],
                "session_token": session_token,
                "signer_name": lead.get("owner_name") or "",
                "signer_email": signing_context["signing_session"].get("sent_to") or "",
            }
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
    else:
        if velvet_engine is None:
            raise HTTPException(
                status_code=503,
                detail=f"Legacy signing engine unavailable in this environment: {VELVET_IMPORT_ERROR}",
            )
        # Generate the draft context using the Velvet Engine
        context = await velvet_engine.create_draft_agreement(dict(lead))
        context["lead_id"] = lead_id
        context["authority_html"] = ""
        context["session_token"] = ""
        context["signer_name"] = lead.get("owner_name") or ""
        context["signer_email"] = ""
    
    template = Template(SIGNING_ROOM_HTML)
    return template.render(**context)

@router.post("/signing/{lead_id}/execute", response_class=HTMLResponse)
async def execute_signature(lead_id: str, request: Request, session: AsyncSession = Depends(get_session)):
    """
    Handles the actual signing event (HTMX POST).
    Generates the final PDF, logs audit trail, and shows success.
    """
    form_data = await request.form()
    if not form_data.get("consent"):
        return """
        <div class="p-4 bg-red-50 text-red-800 rounded-xl border border-red-100 text-sm">
            Please acknowledge the electronic signing consent to proceed.
        </div>
        """

    session_token = form_data.get("session_token")
    if session_token:
        signer_name = (form_data.get("signer_name") or "").strip()
        if not signer_name:
            return """
            <div class="p-4 bg-red-50 text-red-800 rounded-xl border border-red-100 text-sm">
                Enter the vendor name before signing.
            </div>
            """
        try:
            workflow_payload = await complete_signing_session(
                session,
                lead_id,
                str(session_token),
                signer_name=signer_name,
                signer_email=(form_data.get("signer_email") or "").strip(),
                signer_ip=request.client.host if request.client else "",
                signer_user_agent=request.headers.get("user-agent", "unknown"),
            )
            signing_session = workflow_payload.get("latest_signing_session") or {}
            archive_path = signing_session.get("archive_path") or ""
            normalized_archive_path = archive_path.replace("\\", "/")
            download_url = f"/api/forms/download/{normalized_archive_path}" if normalized_archive_path else "#"
            return f"""
            <div class="text-center py-12">
                <div class="text-6xl mb-6">âœ…</div>
                <h1 class="text-3xl font-bold mb-4">Authority Pack Signed</h1>
                <p class="text-gray-500 mb-8">The signed authority pack has been archived and the seller workflow has been updated.</p>
                <a href="{download_url}" class="inline-block px-8 py-4 bg-gray-900 text-white font-bold rounded-2xl">
                    Download Signed Pack
                </a>
            </div>
            """
        except ValueError as exc:
            return f"""
            <div class="p-4 bg-red-50 text-red-800 rounded-xl border border-red-100 text-sm">
                {exc}
            </div>
            """

    # 1. Execute via Velvet Engine
    # In a real app, we'd capture IP/Fingerprint from request headers
    if velvet_engine is None:
        return f"""
        <div class="p-4 bg-red-50 text-red-800 rounded-xl border border-red-100 text-sm">
            Legacy signing engine unavailable in this environment: {VELVET_IMPORT_ERROR}
        </div>
        """
    signed_data = {
        "ip": request.client.host if request.client else "",
        "fingerprint": request.headers.get("user-agent", "unknown"),
        "vendor_name": "Verified Vendor",
        "html_body": "<h1>Final Agreement Content</h1>" # This would be the full legal text
    }
    
    pdf_path = await velvet_engine.execute_agreement(lead_id, signed_data)
    
    # 2. Update Lead Status in DB
    await session.execute(
        text("UPDATE leads SET status = 'signed_agreement', updated_at = :now WHERE id = :id"),
        {"id": lead_id, "now": __import__("datetime").datetime.utcnow().isoformat()}
    )
    await session.commit()

    return f"""
    <div class="text-center py-12">
        <div class="text-6xl mb-6">✅</div>
        <h1 class="text-3xl font-bold mb-4">Agreement Signed!</h1>
        <p class="text-gray-500 mb-8">A fully executed copy has been sent to your email. You can also download it below for your records.</p>
        <a href="/api/forms/download/agreement/{os.path.basename(pdf_path)}" class="inline-block px-8 py-4 bg-gray-900 text-white font-bold rounded-2xl">
            Download Signed Agreement
        </a>
    </div>
    """
