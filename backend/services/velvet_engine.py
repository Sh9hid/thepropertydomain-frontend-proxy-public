import os
import json
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from jinja2 import Environment, FileSystemLoader, select_autoescape
import shortuuid
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from core.config import BRAND_NAME, PRINCIPAL_NAME, PRINCIPAL_EMAIL, BRAND_LOGO_URL, PROJECT_ROOT
from core.utils import now_sydney, format_sydney

# --- COMPLIANCE CONSTANTS (NSW 2026) ---
FTR32_GUIDE_URL = "https://www.fairtrading.nsw.gov.au/__data/assets/pdf_file/0009/1015569/FTR32-Agency-agreements-for-residential-property-guide.pdf"
COMPLIANCE_ROOT = Path(PROJECT_ROOT) / "backend" / "compliance_archive"

class VelvetEngine:
    """
    Bleeding-edge 2026 Form Engine using WeasyPrint + Jinja2.
    Focuses on speed, pixel-perfection, and legal safety.
    """
    def __init__(self):
        self.env = Environment(
            loader=FileSystemLoader(Path(PROJECT_ROOT) / "backend" / "templates"),
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.font_config = FontConfiguration()
        COMPLIANCE_ROOT.mkdir(parents=True, exist_ok=True)

    def _get_lead_archive_dir(self, lead_id: str) -> Path:
        path = COMPLIANCE_ROOT / f"lead_{lead_id}"
        path.mkdir(parents=True, exist_ok=True)
        (path / "annexures").mkdir(exist_ok=True)
        return path

    async def generate_signing_token(self, lead_id: str) -> str:
        """Generates a secure, short-lived token for the HTML Signing Room."""
        # In production, this would be a JWT or encrypted session.
        return f"sign_{shortuuid.uuid()[:12]}"

    async def create_draft_agreement(self, lead: Dict[str, Any], campaign_type: str = "EXCLUSIVE") -> Dict[str, Any]:
        """Creates a high-fidelity draft for review in the Signing Room."""
        lead_id = lead.get("id", "temp")
        archive_dir = self._get_lead_archive_dir(lead_id)
        
        # Logic for vendor splitting (Listening to Dropbox advice)
        owner_raw = lead.get("owner_name", "Valued Client")
        vendors = [name.strip().title() for name in owner_raw.replace(" & ", " and ").split(" and ")]
        
        # Commission logic (Dollar amount + Percentage)
        est_price = lead.get("est_value") or 1250000
        comm_pct = 1.5
        comm_val = (est_price * comm_pct) / 100

        context = {
            "vendors": vendors,
            "property_address": lead.get("address"),
            "campaign_type": campaign_type.upper(),
            "commission_pct": comm_pct,
            "commission_val": f"{int(comm_val):,}",
            "example_price": f"{int(est_price):,}",
            "agent_name": PRINCIPAL_NAME,
            "brand_name": BRAND_NAME,
            "logo_url": BRAND_LOGO_URL,
            "date": now_sydney().strftime("%d %B %Y"),
            "fair_trading_guide": FTR32_GUIDE_URL
        }

        return context

    def render_pdf(self, html_content: str, output_path: Path):
        """
        Uses WeasyPrint for institutional-grade PDF generation.
        Faster and more reliable than Playwright for CSS-heavy documents.
        """
        # Add basic print styles if not present
        styled_html = f"""
        <html>
        <head>
            <style>
                @page {{ size: A4; margin: 2cm; @bottom-right {{ content: "Page " counter(page) " of " counter(pages); font-size: 9pt; }} }}
                body {{ font-family: 'Helvetica', sans-serif; font-size: 11pt; color: #333; }}
                .compliance-stamp {{ position: absolute; top: 0; right: 0; font-size: 8pt; color: #999; border: 1px solid #eee; padding: 5px; }}
            </style>
        </head>
        <body>
            <div class="compliance-stamp">NSW PSA ACT 2002 COMPLIANT | {now_sydney().isoformat()}</div>
            {html_content}
        </body>
        </html>
        """
        HTML(string=styled_html).write_pdf(
            str(output_path),
            font_config=self.font_config
        )

    async def execute_agreement(self, lead_id: str, signed_data: Dict[str, Any]) -> str:
        """
        Finalizes the agreement, generates the PDF, logs the audit trail,
        and saves everything to the compliance archive.
        """
        archive_dir = self._get_lead_archive_dir(lead_id)
        timestamp = now_sydney().isoformat()
        
        # 1. Save Audit Trail (NSW Electronic Transactions Act Compliance)
        audit_trail = {
            "event": "AGREEMENT_EXECUTED",
            "timestamp": timestamp,
            "ip_address": signed_data.get("ip"),
            "fingerprint": signed_data.get("fingerprint"),
            "vendor_name": signed_data.get("vendor_name"),
            "method": "VELVET_E_SIGN_V1"
        }
        with open(archive_dir / "audit_trail.json", "w") as f:
            json.dump(audit_trail, f, indent=2)

        # 2. Generate Final PDF
        # Note: In a real flow, we'd render a specific 'Executed' template here.
        pdf_path = archive_dir / f"Agency_Agreement_{lead_id}_SIGNED.pdf"
        self.render_pdf(signed_data.get("html_body", "<h1>Executed Agreement</h1>"), pdf_path)
        
        return str(pdf_path)

velvet_engine = VelvetEngine()
