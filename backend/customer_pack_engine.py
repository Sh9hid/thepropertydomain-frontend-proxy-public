import asyncio
import html
import json
import sqlite3
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from report_pack_engine import (
    _clean_text,
    _folder_safe,
    _format_date,
    _format_money,
    _format_number,
    AGENT_IMAGE_URL,
    AGENT_PROFILE_URL,
    _metric_grid,
    _render_document,
    _satellite_image_url,
    _subject_fact_grid,
    build_property_bundle,
    build_report_documents,
)


GREETING_DOC_NAMES = {
    "welcome_letter": "Welcome Letter.pdf",
    "why_choose": "Why Choose Laing+Simmons Oakville - Windsor.pdf",
    "meet_nitin": "Meet Nitin Puri.pdf",
    "testimonials": "Client Results & Testimonials.pdf",
    "strategy": "Our Selling Strategy.pdf",
    "opportunity": "Property Opportunity Brief.pdf",
    "ai_visual": "AI Visual Concept Board.pdf",
    "next_steps": "Next Steps To Book Your Appraisal.pdf",
}

RESEARCH_DOC_NAMES = {
    "property_details_sheet": "Property Details Sheet.pdf",
    "sales_cma": "Comparative Market Analysis.pdf",
    "property_profile": "Property Profile Report.pdf",
    "rental_cma": "Rental Comparative Market Analysis.pdf",
    "rental_avm": "Estimated Rental Amount Report.pdf",
    "suburb_profile": "Suburb Profile Report.pdf",
    "suburb_statistics": "Suburb Statistics Report.pdf",
    "sale_avm": "Automated Valuation Estimate.pdf",
}

PRINCIPAL_BIO = (
    "Nitin Puri is the Principal of Laing+Simmons Oakville | Windsor. With extensive experience in the North West "
    "Sydney property market, Nitin leads a team focused on delivering premium results across Oakville, Maraylya, "
    "McGraths Hill, Pitt Town, Windsor, South Windsor and Bligh Park. He previously ran his own boutique agency "
    "and built a reputation for record-breaking results. As a seasoned property investor himself, he brings an "
    "owner's perspective to every transaction and is known for transparency, honesty and a genuine human touch."
)

FEATURED_TESTIMONIALS = [
    ("Integrity", "\"...his genuine human touch was a breath of fresh air. Highly recommend him.\" - Buyer, The Ponds"),
    ("Results", "\"He managed to secure a suburb record for us through sheer persistence and a deep understanding of the local buyer pool.\" - Seller, Riverstone"),
    ("Trust", "\"His conversations were very transparent from day one which makes him very trustworthy.\" - Seller, Box Hill"),
    ("Service", "\"He managed to set the right expectations... and never once pressured us.\" - Buyer, Oakville"),
]

FULL_TESTIMONIALS = [
    ("Marsden Park", "Vendor Review", "Nitin is by far the best real estate agent we've ever worked with. He is honest, professional, savvy, dogged, well-connected and extremely competent. He worked tirelessly to get what he believed was the best deal for us."),
    ("Box Hill", "Vendor Review", "Nitin has shown exceptional skills and dedication in the swift and successful sale of our property. He was on top of all conversations and made sure the promised result was delivered."),
    ("The Ponds", "First Home Buyer", "As first-time buyers, the process felt overwhelming, but Nitin was there at every step, guiding us with honesty and patience."),
    ("Riverstone", "Vendor Review", "Nitin's approach is refreshing. He prioritizes people over profits, and it shows in his negotiation style."),
    ("Oakville", "Land & Home Package Buyer", "He managed to set the right expectations, communicate clearly, and deliver what he promised. He never once pressured us."),
    ("North West Sydney", "Vendor Review", "From the very beginning, he was professional, knowledgeable, and completely dedicated to getting us the best possible outcome."),
    ("Gables", "Investment Buyer", "Professional, trustworthy, and has immense knowledge of the market. He worked tirelessly round the clock to get the deal closed."),
    ("Tallawong", "Land Buyer", "He helped us purchase a block of land at a good price. He is professional, trustworthy, and always available for any discussions."),
]


def _ctx(principal_name: str, principal_email: str, principal_phone: str) -> Dict[str, Any]:
    local_image = Path("D:/L+S nitin.JPG")
    return {
        "brand_name": "Laing+Simmons Oakville | Windsor",
        "brand_area": "Oakville | Windsor",
        "brand_logo_url": "",
        "principal_image_url": local_image.as_uri() if local_image.exists() else AGENT_IMAGE_URL,
        "principal_profile_url": AGENT_PROFILE_URL,
        "principal_name": principal_name,
        "principal_email": principal_email,
        "principal_phone": principal_phone,
    }


def _contact_block(ctx: Dict[str, Any]) -> str:
    return (
        f"<p><strong>{html.escape(ctx['principal_name'])}</strong><br />"
        f"Laing+Simmons Oakville | Windsor<br />"
        f"{html.escape(ctx['principal_phone'])}<br />"
        f"{html.escape(ctx['principal_email'])}</p>"
    )


def _pillar_grid(items: List[Tuple[str, str]]) -> str:
    markup = []
    for label, value in items:
        markup.append(
            f"""
            <div class="pillar">
              <span class="kicker">{html.escape(label)}</span>
              <span class="value">{html.escape(value)}</span>
            </div>
            """
        )
    return f'<div class="pillar-grid">{"".join(markup)}</div>'


def _quote_grid(items: List[Tuple[str, str]]) -> str:
    markup = []
    for title, quote in items:
        markup.append(
            f"""
            <div class="quote-card">
              <p>{html.escape(quote)}</p>
              <strong>{html.escape(title)}</strong>
            </div>
            """
        )
    return f'<div class="quote-grid">{"".join(markup)}</div>'


def _timeline(steps: List[Tuple[str, str]]) -> str:
    markup = []
    for index, (title, body) in enumerate(steps, start=1):
        markup.append(
            f"""
            <div class="timeline-step">
              <div class="dot">{index}</div>
              <div class="body">
                <strong>{html.escape(title)}</strong>
                <p>{html.escape(body)}</p>
              </div>
            </div>
            """
        )
    return f'<div class="timeline">{"".join(markup)}</div>'


def _score_strip(items: List[str]) -> str:
    return '<div class="score-strip">' + "".join(f'<span class="score-chip">{html.escape(item)}</span>' for item in items) + "</div>"


def _welcome_letter(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    subject = bundle["subject"]
    valuation = bundle["valuation"]
    suburb = bundle["suburb"]
    pages = [
        f"""
        <div class="eyebrow">Greeting</div>
        <h1>A tailored property pack, prepared to make the appraisal conversation easier</h1>
        <div class="meta">{html.escape(subject['address'])}</div>
        <div class="meta">Prepared for {html.escape(subject['owner_name'])}</div>
        <div class="hero-panel">
          <div class="hero-copy">
            <p>This pack brings together the strongest property facts, local sales evidence and suburb context currently available so the value discussion starts from substance, not guesswork.</p>
            <p>The current internal estimate for this property sits around <strong>{html.escape(_format_money(valuation['estimate']))}</strong>, subject to condition, presentation, timing and buyer depth at the point of campaign.</p>
            <div class="callout">
              <strong>What you will find inside</strong>
              <p>Property details, comparative sales evidence, suburb context, our selling approach and the practical next steps if you would like a principal-led appraisal.</p>
            </div>
          </div>
          <div class="card soft">
            <h3>Pack Snapshot</h3>
            {_pillar_grid([('Indicative value', _format_money(valuation['estimate'])), ('Comparable sales', str(len(bundle['comparables']))), ('Suburb sales 12m', str(suburb['sales_last_12m']))])}
          </div>
        </div>
        {_contact_block(ctx)}
        """,
        f"""
        <h2>What makes this useful</h2>
        {_pillar_grid([('Clarity', 'A grounded value discussion, not a generic estimate'), ('Context', 'Recent local evidence and suburb movement'), ('Next step', 'A short appraisal review with a clear recommendation')])}
        <h2>How to use this pack</h2>
        {_timeline([('Review the research reports', 'Start with the property details sheet and comparative market analysis to understand the current position.'), ('Read the strategy pages', 'The greeting folder explains why owners choose the team, how campaigns are run and what Nitin focuses on.'), ('Book a short appraisal', 'Use the pack as the starting point for a more precise discussion about likely value and sale strategy.')])}
        """
    ]
    return _render_document("Welcome Letter", subject["address"], pages, ctx["brand_logo_url"], "#11305c")


def _why_choose(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    pages = [
        f"""
        <div class="eyebrow">Why Choose Us</div>
        <h1>Laing+Simmons Oakville | Windsor</h1>
        <p>We focus on owner outcomes, not just transaction volume. Every appraisal, pricing conversation and campaign recommendation is grounded in local evidence, sharp positioning and clear communication.</p>
        {_metric_grid([('Local focus', 'Oakville, Windsor and the Hawkesbury corridor'), ('Approach', 'Transparent and practical'), ('Priority', 'Best possible outcome, not pressure'), ('Direct contact', ctx['principal_phone'])])}
        """,
        f"""
        <h2>What clients can expect</h2>
        {_pillar_grid([('Clear advice', 'Likely value, buyer depth and realistic market position'), ('Direct access', 'Principal-led communication from appraisal through to negotiation'), ('Local familiarity', 'Windsor, Pitt Town, South Windsor, McGraths Hill, Oakville and Bligh Park')])}
        """,
        f"""
        <h2>The difference in practice</h2>
        <p>Preparation, positioning and negotiation matter more than generic portal exposure. The goal is to make every part of the process calmer, clearer and better informed for the owner.</p>
        <div class="callout"><strong>Relevant local focus</strong><p>Oakville, Windsor, South Windsor, Pitt Town, McGraths Hill, Maraylya and Bligh Park remain active owner-occupier and upgrader markets where local buyer understanding makes a practical difference.</p></div>
        {_contact_block(ctx)}
        """,
    ]
    return _render_document("Why Choose Laing+Simmons Oakville | Windsor", "Principal-led overview", pages, ctx["brand_logo_url"], "#11305c")


def _meet_nitin(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    agent_sales = bundle.get("agent_sales") or []
    sold_places = ", ".join(sorted({f"{item.get('suburb')}" for item in agent_sales if item.get("suburb")})[:8])
    pages = [
        f"""
        <div class="eyebrow">Principal Profile</div>
        <h1>Nitin Puri</h1>
        <div class="meta">Principal | Licensee-In-Charge</div>
        <div class="content-grid">
          <div><img class="hero-image" src="{html.escape(ctx['principal_image_url'])}" alt="Nitin Puri" /></div>
          <div><p>{html.escape(PRINCIPAL_BIO)}</p><p class="footer-note">Profile reference: {html.escape(ctx['principal_profile_url'])}</p></div>
        </div>
        {_score_strip(['Principal-led service', 'North West Sydney experience', 'Owner-investor perspective'])}
        {_contact_block(ctx)}
        """,
        """
        <h2>Professional perspective</h2>
        <div class="quote-grid">
          <div class="quote-card"><p>Deep experience across North West Sydney residential property, from owner-occupier homes to strategic sales opportunities.</p><strong>Market depth</strong></div>
          <div class="quote-card"><p>Track record of strong outcomes and record-setting sales built on local understanding, persistence and realistic advice.</p><strong>Results</strong></div>
        </div>
        """,
        """
        <h2>Primary service areas</h2>
        <p>Oakville, Maraylya, McGraths Hill, Pitt Town, Windsor, South Windsor and Bligh Park.</p>
        <p>Recorded sold-property footprint in the current system includes: """
        + html.escape(sold_places or "Windsor district and surrounding North West Sydney suburbs.")
        + """</p>
        <div class="callout"><strong>Working style</strong><p>Transparent conversations, realistic expectations and principal-led oversight from appraisal through to negotiation.</p></div>
        """,
    ]
    return _render_document("Meet Nitin Puri", "Principal profile", pages, ctx["brand_logo_url"], "#11305c")


def _testimonials_doc(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    featured_html = _quote_grid(FEATURED_TESTIMONIALS)
    full_html = "".join(
        f'<div class="card"><h3>{html.escape(area)} | {html.escape(kind)}</h3><p>{html.escape(text)}</p></div>' for area, kind, text in FULL_TESTIMONIALS
    )
    pages = [
        f"""
        <div class="eyebrow">Client Proof</div>
        <h1>Client Results & Testimonials</h1>
        <p>Owners want clarity, trust and a strong result. These excerpts were selected because they speak directly to those three themes.</p>
        {featured_html}
        {_score_strip(['Integrity', 'Results', 'Transparency', 'Service'])}
        <p class="footer-note">Public review reference: {html.escape(ctx['principal_profile_url'])}</p>
        """,
        f"<h2>Testimonial Portfolio</h2>{full_html}",
    ]
    return _render_document("Client Results & Testimonials", "Selected client feedback", pages, ctx["brand_logo_url"], "#11305c")


def _strategy_doc(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    pages = [
        """
        <div class="eyebrow">Selling Strategy</div>
        <h1>How we prepare, position and negotiate for owner outcomes</h1>
        """
        + _timeline([
            ("Preparation and pricing", "The campaign starts with evidence, likely buyer identification and a pricing position that can be defended."),
            ("Launch and buyer management", "Presentation, photography direction and enquiry management are treated as conversion levers, not admin."),
            ("Negotiation and leverage", "Buyer feedback and negotiation pressure are interpreted carefully so the owner understands where real leverage exists."),
            ("Decision and next steps", "Every campaign decision should be clear, timely and anchored to the owner’s actual objectives."),
        ]),
        """
        <h2>Preparation</h2>
        <p>The objective is to bring the property to market in the strongest possible position. That includes pricing discipline, presentation recommendations, photography direction and a launch approach that suits the property type and likely buyer pool.</p>
        <h2>Buyer management</h2>
        <p>Enquiry quality, follow-up speed and buyer qualification are treated as core conversion work, not admin.</p>
        """,
        f"""
        <h2>Negotiation</h2>
        <p>Negotiation is handled with clarity and structure. Owners should know where leverage exists, what buyer pressure is genuine, and how timing affects the outcome.</p>
        {_contact_block(ctx)}
        """,
    ]
    return _render_document("Our Selling Strategy", "Practical campaign methodology", pages, ctx["brand_logo_url"], "#11305c")


def _opportunity_doc(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    subject = bundle["subject"]
    valuation = bundle["valuation"]
    suburb = bundle["suburb"]
    pages = [
        f"""
        <div class="eyebrow">Property Opportunity</div>
        <h1>{html.escape(subject['address'])}</h1>
        {_metric_grid([('Estimated Value', _format_money(valuation['estimate'])), ('Confidence', valuation['confidence']), ('Suburb Median', _format_money(suburb['median_last_12m'])), ('Sales Last 12m', str(suburb['sales_last_12m']))])}
        <p>This brief is designed to frame why an appraisal conversation now is commercially useful for the owner.</p>
        {_score_strip(['Owner-occupied property', 'Active local market', 'Evidence-backed appraisal opening'])}
        """,
        f"""
        <h2>Why this property is worth reviewing now</h2>
        {_pillar_grid([('Evidence', 'Documented local sales evidence exists in the archive'), ('Market setting', 'The property sits within an active owner-occupier market'), ('Potential upside', 'An appraisal can identify timing, positioning and presentation advantages')])}
        """,
        f"""
        <h2>Discussion points for the owner</h2>
        <p>Likely value range, current market pace, who the buyer is likely to be, and whether there is an opportunity to move before competition increases.</p>
        {_contact_block(ctx)}
        """,
    ]
    return _render_document("Property Opportunity Brief", subject["address"], pages, ctx["brand_logo_url"], "#11305c")


def _ai_visual_doc(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    subject = bundle["subject"]
    satellite_url = _satellite_image_url(subject)
    pages = [
        f"""
        <div class="eyebrow">AI Visual Concept Board</div>
        <h1>Presentation and visual direction</h1>
        <p>This file frames how AI-assisted visuals would be used to support a future campaign without replacing factual reporting.</p>
        <div class="callout">
          <strong>Use of AI visuals</strong>
          <p>Concept imagery should sit in the marketing strategy layer only. It should never replace the factual property reports or be confused with real site photography.</p>
        </div>
        {_score_strip(['Concept imagery only', 'Separate from factual reporting', 'Used for campaign positioning'])}
        """,
        (
            f"<h2>Location board</h2><img class=\"hero-image\" src=\"{html.escape(satellite_url)}\" alt=\"Satellite view of {html.escape(subject['address'])}\" /><p class=\"footer-note\">Recorded property coordinates used for the marked location view.</p>"
            if satellite_url
            else "<h2>Location board</h2><div class=\"empty-state\">No verified coordinates were available for this property.</div>"
        ),
        f"""
        <h2>How we would use AI carefully</h2>
        {_pillar_grid([('Campaign moodboards', 'To help shape positioning and creative direction'), ('Styling concepts', 'To explore presentation ideas after owner approval'), ('Strict separation', 'Concept imagery remains separate from factual site photography')])}
        {_contact_block(ctx)}
        """,
    ]
    return _render_document("AI Visual Concept Board", subject["address"], pages, ctx["brand_logo_url"], "#11305c")


def _next_steps_doc(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    subject = bundle["subject"]
    pages = [
        f"""
        <div class="eyebrow">Next Steps</div>
        <h1>How to book your appraisal</h1>
        <p>The next step is a short appraisal conversation focused on likely value, buyer depth, timing and what would improve the outcome if you decided to move.</p>
        {_metric_grid([('Property', subject['address']), ('Principal', ctx['principal_name']), ('Phone', ctx['principal_phone']), ('Email', ctx['principal_email'])])}
        """,
        f"""
        <h2>What we will cover</h2>
        {_timeline([('Indicative value range', 'A more precise discussion of where the property is likely to sit today.'), ('Current competition', 'A practical read on nearby competition and buyer activity.'), ('Likely buyer profile', 'Who the likely buyer is and what matters most to them.'), ('Preparation and campaign options', 'What would improve the outcome if the property were brought to market.')])}
        {_contact_block(ctx)}
        """,
    ]
    return _render_document("Next Steps To Book Your Appraisal", subject["address"], pages, ctx["brand_logo_url"], "#11305c")


def build_greeting_documents(bundle: Dict[str, Any], ctx: Dict[str, Any]) -> List[Tuple[str, str]]:
    return [
        (GREETING_DOC_NAMES["welcome_letter"], _welcome_letter(bundle, ctx)),
        (GREETING_DOC_NAMES["why_choose"], _why_choose(bundle, ctx)),
        (GREETING_DOC_NAMES["meet_nitin"], _meet_nitin(bundle, ctx)),
        (GREETING_DOC_NAMES["testimonials"], _testimonials_doc(bundle, ctx)),
        (GREETING_DOC_NAMES["strategy"], _strategy_doc(bundle, ctx)),
        (GREETING_DOC_NAMES["opportunity"], _opportunity_doc(bundle, ctx)),
        (GREETING_DOC_NAMES["ai_visual"], _ai_visual_doc(bundle, ctx)),
        (GREETING_DOC_NAMES["next_steps"], _next_steps_doc(bundle, ctx)),
    ]


async def create_customer_pack(
    conn: sqlite3.Connection,
    lead: Dict[str, Any],
    *,
    stock_root: str,
    principal_name: str,
    principal_email: str,
    principal_phone: str,
    html_to_pdf: Callable[[str, str], Awaitable[Any]],
    output_root: Optional[Path] = None,
) -> Dict[str, Any]:
    bundle = build_property_bundle(conn, lead, stock_root)
    subject = bundle["subject"]
    root = (output_root or Path("D:/")) / _folder_safe(subject["address"])
    research_dir = root / "Research Reports"
    greeting_dir = root / "Greeting"
    for directory in (research_dir, greeting_dir):
        directory.mkdir(parents=True, exist_ok=True)

    ctx = _ctx(principal_name, principal_email, principal_phone)
    research_artifacts = []
    for temp_filename, doc_type, html_content in build_report_documents(bundle, ctx):
        final_name = RESEARCH_DOC_NAMES.get(doc_type, temp_filename)
        output_path = research_dir / final_name
        await html_to_pdf(html_content, str(output_path))
        research_artifacts.append({"type": doc_type, "filename": final_name, "path": str(output_path)})

    for file_name, html_content in build_greeting_documents(bundle, ctx):
        await html_to_pdf(html_content, str(greeting_dir / file_name))

    welcome_email = (
        f"Subject: Property pack for {subject['address']}\n\n"
        f"Dear {subject['owner_name']},\n\n"
        "Please find attached a tailored property pack prepared for your property. It includes a factual property summary, "
        "comparative market analysis, suburb context and a short introduction to our approach.\n\n"
        "If helpful, I would be glad to walk you through the likely value position, buyer interest and the strongest next steps in a short appraisal conversation.\n\n"
        f"{principal_name}\nPrincipal | Licensee-In-Charge\nLaing+Simmons Oakville | Windsor\n{principal_phone}\n{principal_email}\n"
    )
    (greeting_dir / "Welcome Email.txt").write_text(welcome_email, encoding="utf-8")
    welcome_email_html = "\n".join(
        [
            "<!doctype html>",
            "<html lang=\"en\">",
            "  <head>",
            "    <meta charset=\"utf-8\" />",
            f"    <title>Property pack for {html.escape(subject['address'])}</title>",
            "    <style>",
            "      body { font-family: Georgia, 'Times New Roman', serif; color: #122033; background: #f5f7fb; padding: 32px; }",
            "      .email { max-width: 760px; margin: 0 auto; background: white; border: 1px solid #dbe3ee; padding: 36px; }",
            "      .brand { font-family: 'Segoe UI', Arial, sans-serif; font-size: 30px; font-weight: 700; color: #11305c; }",
            "      .brand .plus { color: #d6a84f; }",
            "      .area { font-family: 'Segoe UI', Arial, sans-serif; color: #56677b; letter-spacing: .14em; text-transform: uppercase; font-size: 12px; margin-top: 4px; }",
            "      p { font-size: 15px; line-height: 1.6; margin: 0 0 14px; }",
            "      .cta { margin-top: 24px; padding-top: 18px; border-top: 1px solid #dbe3ee; }",
            "    </style>",
            "  </head>",
            "  <body>",
            "    <div class=\"email\">",
            "      <div class=\"brand\">Laing<span class=\"plus\">+</span>Simmons</div>",
            "      <div class=\"area\">Oakville | Windsor</div>",
            f"      <p>Dear {html.escape(subject['owner_name'])},</p>",
            f"      <p>Please find attached a tailored property pack prepared for <strong>{html.escape(subject['address'])}</strong>. It brings together factual property information, comparative sales evidence, suburb context and a brief overview of how we approach pricing and negotiation.</p>",
            "      <p>If useful, I would be happy to walk you through the likely value position, current buyer depth and the strongest next steps in a short appraisal conversation.</p>",
            "      <div class=\"cta\">",
            f"        <p><strong>{html.escape(principal_name)}</strong><br />Principal | Licensee-In-Charge<br />Laing+Simmons Oakville | Windsor<br />{html.escape(principal_phone)}<br />{html.escape(principal_email)}</p>",
            "      </div>",
            "    </div>",
            "  </body>",
            "</html>",
        ]
    )
    (greeting_dir / "Welcome Email.html").write_text(welcome_email_html, encoding="utf-8")

    manifest = {
        "address": subject["address"],
        "owner_name": subject["owner_name"],
        "pack_root": str(root),
        "research_reports": research_artifacts,
        "greeting_files": sorted(path.name for path in greeting_dir.iterdir()),
        "source_summary": {
            "comparables": len(bundle["comparables"]),
            "sales_last_12m": bundle["suburb"]["sales_last_12m"],
            "suburb_records": bundle["suburb"]["records_observed"],
        },
        "principal_contact": {
            "name": principal_name,
            "email": principal_email,
            "phone": principal_phone,
        },
    }
    (root / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def run_customer_pack_generation_sync(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    return asyncio.run(create_customer_pack(*args, **kwargs))
