import os
import json
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from jinja2 import Template
from core.config import (
    BRAND_NAME,
    BRAND_LOGO_URL,
    GENERATED_REPORTS_ROOT,
    PRINCIPAL_EMAIL,
    PRINCIPAL_NAME,
    PRINCIPAL_PHONE,
    SYDNEY_TZ,
)
from core.utils import now_sydney, format_sydney
from pdf_generator import html_to_pdf

# --- COMPLIANCE DEFAULTS ---
ACCOMPANIED_TEXT = "Prospective purchasers are to be accompanied through the property by a representative of the agency nominated on this agreement."
SETTLEMENT_TEXT = "To be advised prior to settlement (BSB & Account TBA)"

AGENCY_AGREEMENT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: 'Helvetica', sans-serif; color: #1a1a1a; line-height: 1.4; padding: 40px; }
        .header { border-bottom: 2px solid #E21937; padding-bottom: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: flex-end; }
        .logo { max-height: 60px; }
        .title { color: #E21937; font-size: 24px; font-weight: bold; text-transform: uppercase; }
        .section { margin-bottom: 25px; }
        .section-title { background: #f4f4f4; padding: 5px 10px; font-weight: bold; font-size: 14px; text-transform: uppercase; margin-bottom: 10px; border-left: 4px solid #E21937; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .field { margin-bottom: 8px; font-size: 12px; }
        .label { color: #666; font-weight: bold; width: 150px; display: inline-block; }
        .value { border-bottom: 1px solid #ddd; flex: 1; padding-bottom: 2px; }
        .footer { margin-top: 50px; font-size: 10px; color: #999; border-top: 1px solid #eee; pt: 10px; }
        .signature-box { margin-top: 40px; display: flex; gap: 50px; }
        .sig { border-top: 1px solid #000; width: 250px; padding-top: 5px; font-size: 11px; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="title">{{ agreement_type }} AGENCY AGREEMENT</div>
            <div style="font-size: 12px; color: #666;">Property and Stock Agents Act 2002</div>
        </div>
        <img src="{{ logo_url }}" class="logo">
    </div>

    <div class="section">
        <div class="section-title">1. THE PRINCIPAL (THE VENDOR)</div>
        {% for vendor in vendors %}
        <div class="field"><span class="label">Full Legal Name:</span> <span class="value">{{ vendor.name }}</span></div>
        <div class="field"><span class="label">GST Registered:</span> <span class="value">{{ vendor.gst }}</span></div>
        {% if not loop.last %}<hr style="border: 0; border-top: 1px dashed #eee; margin: 10px 0;">{% endif %}
        {% endfor %}
        <div class="field" style="margin-top: 10px;"><span class="label">Address:</span> <span class="value">{{ address }}</span></div>
    </div>

    <div class="section">
        <div class="section-title">2. THE AGENT</div>
        <div class="field"><span class="label">Agency Name:</span> <span class="value">{{ brand_name }}</span></div>
        <div class="field"><span class="label">Principal:</span> <span class="value">{{ agent_name }}</span></div>
        <div class="field"><span class="label">Contact:</span> <span class="value">{{ agent_phone }} | {{ agent_email }}</span></div>
    </div>

    <div class="section">
        <div class="section-title">3. AGENT'S REMUNERATION (COMMISSION)</div>
        <div class="field">
            <span class="label">Agreed Rate:</span> 
            <span class="value">{{ commission_rate }}% of the sale price.</span>
        </div>
        <div class="field">
            <span class="label">Estimated Fee:</span> 
            <span class="value">As an example, if the property sells for ${{ example_price }}, the fee would be ${{ estimated_fee }} (incl. GST).</span>
        </div>
    </div>

    <div class="section">
        <div class="section-title">4. INSPECTIONS & MARKETING</div>
        <div class="field">
            <span class="label">Inspections:</span> 
            <span class="value">{{ accompanied_text }}</span>
        </div>
        <div class="field">
            <span class="label">Marketing:</span> 
            <span class="value">As per the attached Marketing Schedule (Annexure A).</span>
        </div>
    </div>

    <div class="section">
        <div class="section-title">10. SETTLEMENT OF MONIES</div>
        <div class="field">
            <span class="label">Payment Instructions:</span> 
            <span class="value">{{ settlement_text }}</span>
        </div>
    </div>

    <div class="section">
        <div class="section-title">WAIVE COOLING OFF PERIOD</div>
        <div class="field">
            <span class="label">Waive Period:</span> 
            <span class="value">NO</span>
        </div>
    </div>

    <div class="signature-box">
        <div class="sig">Principal (Vendor) Signature<br>Date: {{ date_today }}</div>
        <div class="sig">Agent Signature<br>Date: {{ date_today }}</div>
    </div>

    <div class="footer">
        Generated by {{ brand_name }} Intelligence Hub | Reference: {{ lead_id }} | Sydney Time: {{ timestamp }}
    </div>
</body>
</html>
"""

async def generate_agency_agreement(lead: Dict[str, Any], campaign_type: str = "EXCLUSIVE") -> str:
    """
    Generates a PDF Agency Agreement based on compliance rules.
    campaign_type: 'EXCLUSIVE' or 'AUCTION'
    """
    # 1. Handle Vendors (Split Names)
    # If owner_name contains "And" or "&", we split them.
    owner_raw = lead.get("owner_name", "Valued Client")
    vendors = []
    if " and " in owner_raw.lower():
        names = owner_raw.lower().split(" and ")
    elif " & " in owner_raw:
        names = owner_raw.split(" & ")
    else:
        names = [owner_raw]
    
    for name in names:
        vendors.append({
            "name": name.strip().title(),
            "gst": "NO" # Default to No per standard residential
        })

    # 2. Commission Math
    est_value = lead.get("est_value", 0)
    if est_value == 0:
        est_value = 1250000 # Default example
    
    comm_rate = 1.5 # Standard rate for L+S Oakville
    est_fee = (est_value * comm_rate) / 100
    
    # 3. Agreement Type Logic
    final_type = "AUCTION" if campaign_type.upper() == "AUCTION" else "EXCLUSIVE"

    # 4. Render Template
    template = Template(AGENCY_AGREEMENT_TEMPLATE)
    html_content = template.render(
        agreement_type=final_type,
        vendors=vendors,
        address=lead.get("address", "Property Address"),
        brand_name=BRAND_NAME,
        agent_name=PRINCIPAL_NAME,
        agent_phone=PRINCIPAL_PHONE,
        agent_email=PRINCIPAL_EMAIL,
        logo_url=BRAND_LOGO_URL,
        commission_rate=comm_rate,
        example_price=f"{est_value:,}",
        estimated_fee=f"{int(est_fee):,}",
        accompanied_text=ACCOMPANIED_TEXT,
        settlement_text=SETTLEMENT_TEXT,
        lead_id=lead.get("id", "UNKNOWN"),
        date_today=now_sydney().strftime("%d / %m / %Y"),
        timestamp=format_sydney(now_sydney())
    )

    # 5. Output to File
    output_dir = GENERATED_REPORTS_ROOT / "agreements"
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"Agency_Agreement_{lead.get('id', 'temp')}_{final_type}.pdf"
    output_path = output_dir / filename
    
    await html_to_pdf(html_content, str(output_path))
    
    return str(output_path)

OFFER_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: 'Helvetica', sans-serif; color: #1a1a1a; line-height: 1.4; padding: 40px; }
        .header { border-bottom: 2px solid #E21937; padding-bottom: 20px; margin-bottom: 30px; display: flex; justify-content: space-between; align-items: flex-end; }
        .logo { max-height: 60px; }
        .title { color: #E21937; font-size: 24px; font-weight: bold; text-transform: uppercase; }
        .section { margin-bottom: 25px; }
        .section-title { background: #f4f4f4; padding: 5px 10px; font-weight: bold; font-size: 14px; text-transform: uppercase; margin-bottom: 10px; border-left: 4px solid #E21937; }
        .field { margin-bottom: 12px; font-size: 14px; display: flex; }
        .label { color: #666; font-weight: bold; width: 180px; }
        .value { border-bottom: 1px solid #eee; flex: 1; padding-bottom: 2px; font-weight: 500; }
        .footer { margin-top: 50px; font-size: 10px; color: #999; border-top: 1px solid #eee; pt: 10px; }
        .note { background: #fff9c4; padding: 15px; border-radius: 5px; font-size: 12px; margin-top: 20px; color: #555; }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <div class="title">OFFER TO PURCHASE</div>
            <div style="font-size: 12px; color: #666;">Formal Expression of Interest</div>
        </div>
        <img src="{{ logo_url }}" class="logo">
    </div>

    <div class="section">
        <div class="section-title">PROPERTY DETAILS</div>
        <div class="field"><span class="label">Address:</span> <span class="value">{{ address }}</span></div>
    </div>

    <div class="section">
        <div class="section-title">PURCHASER DETAILS</div>
        <div class="field"><span class="label">Purchaser Name(s):</span> <span class="value">{{ purchaser_name }}</span></div>
        <div class="field"><span class="label">Contact Number:</span> <span class="value">{{ purchaser_phone }}</span></div>
        <div class="field"><span class="label">Solicitor/Conveyancer:</span> <span class="value">{{ purchaser_solicitor }}</span></div>
    </div>

    <div class="section">
        <div class="section-title">OFFER TERMS</div>
        <div class="field"><span class="label">Purchase Price:</span> <span class="value" style="font-size: 18px; color: #E21937;">${{ offer_price }}</span></div>
        <div class="field"><span class="label">Deposit Amount:</span> <span class="value">{{ deposit_amount }}</span></div>
        <div class="field"><span class="label">Settlement Period:</span> <span class="value">{{ settlement_days }} Days</span></div>
        <div class="field"><span class="label">Subject to Finance:</span> <span class="value">{{ subject_to_finance }}</span></div>
        <div class="field"><span class="label">Other Conditions:</span> <span class="value">{{ conditions }}</span></div>
    </div>

    <div class="note">
        <strong>IMPORTANT NOTE:</strong> This offer is not a legally binding contract for sale. It is an expression of interest to purchase the property on the terms stated above. Contracts are only legally binding once signed by both parties and exchanged.
    </div>

    <div class="signature-box" style="margin-top: 40px; display: flex; gap: 50px;">
        <div style="border-top: 1px solid #000; width: 250px; padding-top: 5px; font-size: 11px;">Purchaser Signature<br>Date: {{ date_today }}</div>
    </div>

    <div class="footer">
        Generated by {{ brand_name }} Intelligence Hub | Ref: {{ lead_id }} | {{ timestamp }}
    </div>
</body>
</html>
"""

async def generate_offer_form(lead: Dict[str, Any], offer_data: Dict[str, Any]) -> str:
    """
    Generates a formal Offer to Purchase PDF.
    """
    template = Template(OFFER_TEMPLATE)
    html_content = template.render(
        address=lead.get("address", "Property Address"),
        purchaser_name=offer_data.get("name", "____________________"),
        purchaser_phone=offer_data.get("phone", "____________________"),
        purchaser_solicitor=offer_data.get("solicitor", "TBA"),
        offer_price=f"{int(offer_data.get('price', 0)):,}",
        deposit_amount=offer_data.get("deposit", "0.25% Initial / 10% Balance"),
        settlement_days=offer_data.get("settlement", "42"),
        subject_to_finance=offer_data.get("finance", "NO"),
        conditions=offer_data.get("conditions", "Nil"),
        brand_name=BRAND_NAME,
        logo_url=BRAND_LOGO_URL,
        lead_id=lead.get("id", "UNKNOWN"),
        date_today=now_sydney().strftime("%d / %m / %Y"),
        timestamp=format_sydney(now_sydney())
    )

    output_dir = GENERATED_REPORTS_ROOT / "offers"
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"Offer_{lead.get('id', 'temp')}_{offer_data.get('name', 'Buyer').replace(' ', '_')}.pdf"
    output_path = output_dir / filename
    
    await html_to_pdf(html_content, str(output_path))
    
    return str(output_path)
