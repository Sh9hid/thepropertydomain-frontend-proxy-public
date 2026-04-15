"""
AI-generated hero image for land listings.

Uses Google Gemini 2.0 Flash image generation (or Imagen 3) to produce a
marketing-style hero render of a land block with context like suburb name,
lot size, and archetype.  Saves to LISTING_PHOTOS_ROOT so the same /listing_photos
static mount serves it.

Falls back gracefully if GEMINI_API_KEY is missing or the call fails.
Images are cached per lead_id to avoid regeneration cost.
"""

from __future__ import annotations

import base64
import logging
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _get_listing_photos_root() -> Path:
    from core.config import LISTING_PHOTOS_ROOT
    return LISTING_PHOTOS_ROOT


def _build_prompt(
    suburb: str,
    land_sqm: float,
    archetype: str,
    lot_type: str = "",
    style: str = "lifestyle",
) -> str:
    """Construct a marketing-grade prompt for the hero image."""
    archetype_flavour = {
        "first_home": "a bright, inviting family scene with children playing on freshly levelled ground",
        "investor_yield": "a crisp, professional real estate marketing shot emphasising build-ready land",
        "upgrader_family": "a premium aspirational shot of a large, level block with open skies and landscaping",
        "corner_block": "a wide corner block view showing two street frontages and generous scale",
        "cul_de_sac": "a peaceful cul-de-sac setting with established trees and families walking",
        "family_build": "a warm family-oriented view of a cleared build-ready block with community feel",
    }.get(archetype, "a premium real estate marketing shot of a build-ready land block")

    style_cue = {
        "lifestyle": "golden hour lighting, soft cinematic colour grade, aspirational magazine quality, family lifestyle vibe",
        "investor": "bright clean daylight, crisp architectural real estate style, professional listing quality",
    }.get(style, "golden hour lighting, cinematic quality")

    lot_note = f" The lot is a {lot_type.lower()}." if lot_type else ""

    return (
        f"Ultra-realistic photograph of {archetype_flavour} in the suburb of {suburb}, "
        f"New South Wales, Australia. A cleared, build-ready {int(land_sqm)}sqm residential "
        f"land block ready for construction.{lot_note} "
        f"Modern masterplanned community backdrop with distant rooflines of newly built family homes, "
        f"wide open skies, lush grass, and gentle slope. "
        f"No signage, no logos, no text, no watermarks, no people's faces visible. "
        f"Shot on a 35mm lens from street level. {style_cue}. "
        f"Photorealistic, high resolution, legal for real estate marketing use in Australia."
    )


def _build_thumbnail_prompt(
    suburb: str,
    postcode: str,
    address: str,
    lot_number: str,
    land_sqm: float,
    price: int,
) -> str:
    """MrBeast-style hook thumbnail that still reads as REA-compliant.

    Heavy vignette, golden highlight on the parcel, muted surroundings, bold
    price + sqm call-outs.  No people, no faces, no misleading claims.
    """
    price_txt = f"${price:,}" if price else "Priced to Sell"
    lot_txt = f"LOT {lot_number} " if lot_number else ""
    return (
        f"High-end aerial real estate hero image of a vacant residential land parcel in "
        f"{suburb} {postcode}, New South Wales, Australia. "
        f"Photoreal satellite-style bird's eye view at 300m altitude, late afternoon golden hour lighting. "
        f"The highlighted build-ready {int(land_sqm)}sqm rectangular lot is vivid, saturated green with "
        f"crisp premium golden border glow, soft inner shadow, subtle parcel outline. "
        f"Everything OUTSIDE the parcel is desaturated near-monochrome slate grey to make the lot pop. "
        f"Bold clean uppercase text overlay — top-left: '{lot_txt}{suburb.upper()} LAND OPPORTUNITY'. "
        f"Top-right: professional metadata badges showing '{int(land_sqm)} SQM' and '{price_txt}'. "
        f"Bottom-left: '{address}' in smaller clean sans-serif. "
        f"No people, no faces, no real estate agent logos, no watermarks, no fake signs, no misleading dimensions. "
        f"Premium ad-ready visual suitable for Australian REA portal hero thumbnail. "
        f"Cinematic colour grade, shallow vignette, crisp focus, magazine-quality output, "
        f"dopamine-forward retention hook while remaining factually accurate and REA guideline-compliant. "
        f"Output as clean ad-ready JPG, 16:9 aspect ratio."
    )


def _build_lifestyle_prompt(
    suburb: str,
    postcode: str,
    land_sqm: float,
    archetype: str,
    lot_type: str,
) -> str:
    """Aspirational ground-level lifestyle hero — warmer, softer."""
    return _build_prompt(suburb, land_sqm, archetype, lot_type, style="lifestyle")


def _build_investor_prompt(
    suburb: str,
    postcode: str,
    land_sqm: float,
    archetype: str,
    lot_type: str,
) -> str:
    """Clean professional daylight shot — crisp, functional, investor-ready."""
    return _build_prompt(suburb, land_sqm, archetype, lot_type, style="investor")


def generate_variants(
    lead_id: str,
    suburb: str,
    postcode: str,
    address: str,
    lot_number: str,
    land_sqm: float,
    price: int,
    archetype: str = "family_build",
    lot_type: str = "",
) -> dict:
    """Generate three hero image variants so the operator can pick a winner.

    Returns ``{"thumbnail": url | None, "lifestyle": url | None, "investor": url | None}``.
    Each slot falls back to ``None`` if generation fails so the UI can still render.
    """
    try:
        from core.config import GEMINI_API_KEY
    except ImportError:
        return {"thumbnail": None, "lifestyle": None, "investor": None}

    if not GEMINI_API_KEY:
        return {"thumbnail": None, "lifestyle": None, "investor": None}

    photos_root = _get_listing_photos_root()
    lead_dir = photos_root / lead_id
    lead_dir.mkdir(parents=True, exist_ok=True)

    variants = {
        "thumbnail": _build_thumbnail_prompt(suburb, postcode, address, lot_number, land_sqm, price),
        "lifestyle": _build_lifestyle_prompt(suburb, postcode, land_sqm, archetype, lot_type),
        "investor": _build_investor_prompt(suburb, postcode, land_sqm, archetype, lot_type),
    }

    results: dict = {}
    for variant_name, prompt in variants.items():
        out_file = lead_dir / f"hero_{variant_name}.png"
        url = _run_gemini_image(prompt, out_file, lead_id, variant_name)
        results[variant_name] = url if url else None
        if url:
            results[variant_name] = f"/listing_photos/{lead_id}/hero_{variant_name}.png"
    return results


def _run_gemini_image(prompt: str, out_file: Path, lead_id: str, label: str) -> Optional[str]:
    """Run the Gemini model chain and write the first image returned to ``out_file``."""
    try:
        from core.config import GEMINI_API_KEY
        from google import genai as _genai_new
        from google.genai import types as _genai_types
    except ImportError:
        return None

    if not GEMINI_API_KEY:
        return None

    client = _genai_new.Client(api_key=GEMINI_API_KEY)
    candidate_models = [
        "gemini-3-pro-image-preview",
        "gemini-3.1-flash-image-preview",
        "gemini-2.5-flash-image",
    ]
    for model_name in candidate_models:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=_genai_types.GenerateContentConfig(
                    response_modalities=["TEXT", "IMAGE"],
                ),
            )
            for part in response.candidates[0].content.parts:
                if getattr(part, "inline_data", None) and part.inline_data.data:
                    data = part.inline_data.data
                    if isinstance(data, str):
                        data = base64.b64decode(data)
                    out_file.write_bytes(data)
                    logger.info("%s image generated via %s for %s", label, model_name, lead_id)
                    return str(out_file)
        except Exception as exc:
            logger.warning("%s gen via %s failed for %s: %s", label, model_name, lead_id, exc)
            continue

    try:
        imagen_resp = client.models.generate_images(
            model="imagen-4.0-generate-001",
            prompt=prompt,
            config=_genai_types.GenerateImagesConfig(
                number_of_images=1,
                aspect_ratio="16:9",
            ),
        )
        if imagen_resp.generated_images:
            image_bytes = imagen_resp.generated_images[0].image.image_bytes
            out_file.write_bytes(image_bytes)
            logger.info("%s image generated via imagen-4.0 for %s", label, lead_id)
            return str(out_file)
    except Exception as exc:
        logger.warning("%s imagen-4.0 fallback failed for %s: %s", label, lead_id, exc)

    return None


def generate_hero_image(
    lead_id: str,
    suburb: str,
    land_sqm: float,
    archetype: str = "family_build",
    lot_type: str = "",
    style: str = "lifestyle",
    force: bool = False,
) -> Optional[str]:
    """Generate (or return cached) hero image URL for a listing.

    Returns the URL path (``/listing_photos/{lead_id}/hero_{style}.png``)
    or ``None`` if generation fails.
    """
    try:
        from core.config import GEMINI_API_KEY
    except ImportError:
        logger.warning("GEMINI_API_KEY config not available")
        return None

    if not GEMINI_API_KEY:
        logger.info("GEMINI_API_KEY not set — skipping image generation for %s", lead_id)
        return None

    photos_root = _get_listing_photos_root()
    lead_dir = photos_root / lead_id
    lead_dir.mkdir(parents=True, exist_ok=True)

    out_file = lead_dir / f"hero_{style}.png"
    relative_url = f"/listing_photos/{lead_id}/hero_{style}.png"

    if out_file.exists() and not force:
        return relative_url

    prompt = _build_prompt(suburb, land_sqm, archetype, lot_type, style)

    # Try Gemini image models in order of preference.  Flash is cheapest/fastest
    # while pro gives the best photoreal results.  We fall back down the chain on
    # 404/quota/safety errors so a single model outage never breaks generation.
    candidate_models = [
        "gemini-3-pro-image-preview",
        "gemini-3.1-flash-image-preview",
        "gemini-2.5-flash-image",
    ]

    try:
        from google import genai as _genai_new
        from google.genai import types as _genai_types
        client = _genai_new.Client(api_key=GEMINI_API_KEY)
        for model_name in candidate_models:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=_genai_types.GenerateContentConfig(
                        response_modalities=["TEXT", "IMAGE"],
                    ),
                )
                for part in response.candidates[0].content.parts:
                    if getattr(part, "inline_data", None) and part.inline_data.data:
                        data = part.inline_data.data
                        if isinstance(data, str):
                            data = base64.b64decode(data)
                        out_file.write_bytes(data)
                        logger.info("hero image generated via %s for %s", model_name, lead_id)
                        return relative_url
                logger.info("model %s returned no image parts for %s", model_name, lead_id)
            except Exception as exc:
                logger.warning("image gen model %s failed for %s: %s", model_name, lead_id, exc)
                continue

        # Last resort: imagen-4 via predict() for pure text-to-image
        try:
            imagen_resp = client.models.generate_images(
                model="imagen-4.0-generate-001",
                prompt=prompt,
                config=_genai_types.GenerateImagesConfig(
                    number_of_images=1,
                    aspect_ratio="16:9",
                ),
            )
            if imagen_resp.generated_images:
                img = imagen_resp.generated_images[0]
                image_bytes = img.image.image_bytes
                out_file.write_bytes(image_bytes)
                logger.info("hero image generated via imagen-4.0 for %s", lead_id)
                return relative_url
        except Exception as exc:
            logger.warning("imagen-4.0 fallback failed for %s: %s", lead_id, exc)

    except ImportError:
        logger.warning("google-genai client not installed — cannot generate hero image")
    except Exception as exc:
        logger.exception("hero image generation error for %s: %s", lead_id, exc)

    return None
