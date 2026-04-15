from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException
from core.security import get_api_key
from fastapi import Depends

router = APIRouter()

DOCS_DIR = Path(__file__).resolve().parents[2] / "docs"

_SLUG_ORDER = ["architecture", "leads", "outreach", "scoring", "feeds", "control", "hosting", "api"]


def _slug_from_path(p: Path) -> str:
    return p.stem


def _title_from_slug(slug: str) -> str:
    return slug.replace("_", " ").replace("-", " ").title()


@router.get("/api/docs-content")
async def list_docs(api_key: str = Depends(get_api_key)) -> Dict[str, Any]:
    """Return all documentation pages as {slug, title, content}."""
    if not DOCS_DIR.exists():
        return {"pages": []}

    pages = []
    for md_file in sorted(
        DOCS_DIR.glob("*.md"),
        key=lambda p: (_SLUG_ORDER.index(p.stem) if p.stem in _SLUG_ORDER else 99, p.stem),
    ):
        slug = _slug_from_path(md_file)
        content = md_file.read_text(encoding="utf-8")
        pages.append({
            "slug": slug,
            "title": _title_from_slug(slug),
            "content": content,
        })
    return {"pages": pages}


@router.get("/api/docs-content/{slug}")
async def get_doc(slug: str, api_key: str = Depends(get_api_key)) -> Dict[str, Any]:
    """Return a single documentation page by slug."""
    md_file = DOCS_DIR / f"{slug}.md"
    if not md_file.exists():
        raise HTTPException(status_code=404, detail=f"Doc '{slug}' not found")
    content = md_file.read_text(encoding="utf-8")
    return {
        "slug": slug,
        "title": _title_from_slug(slug),
        "content": content,
    }
