"""
OCR Service — extracts text from scanned PDFs using Tesseract.
Falls back to pypdf text layer first; only invokes Tesseract when
pypdf returns fewer than 50 meaningful characters per page.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

MIN_CHARS_THRESHOLD = 50  # below this, assume scanned page


class OCRService:
    """
    Hybrid PDF text extractor:
      1. Try pypdf native text layer (fast, no dependencies)
      2. If insufficient, convert page to image and run Tesseract
    """

    def extract_text(self, pdf_path: Path) -> str:
        """Return full text extracted from the PDF at pdf_path."""
        pdf_path = Path(pdf_path)
        if not pdf_path.exists():
            logger.warning(f"[OCR] File not found: {pdf_path}")
            return ""

        try:
            text = self._pypdf_extract(pdf_path)
        except Exception as exc:
            logger.debug(f"[OCR] pypdf failed for {pdf_path.name}: {exc}")
            text = ""

        if len(text.strip()) >= MIN_CHARS_THRESHOLD:
            return text

        # Fall back to Tesseract OCR
        logger.info(f"[OCR] Falling back to Tesseract for {pdf_path.name}")
        try:
            return self._tesseract_extract(pdf_path)
        except Exception as exc:
            logger.warning(f"[OCR] Tesseract failed for {pdf_path.name}: {exc}")
            return text  # return whatever pypdf managed

    def _pypdf_extract(self, pdf_path: Path) -> str:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        return "\n".join(parts)

    def _tesseract_extract(self, pdf_path: Path) -> str:
        """Convert each PDF page to image and run Tesseract."""
        try:
            import pytesseract
            from pdf2image import convert_from_path
            from PIL import Image
        except ImportError as exc:
            raise RuntimeError(
                f"OCR dependencies missing: {exc}. "
                "Install pytesseract, pdf2image, and Pillow, plus Tesseract binary."
            ) from exc

        pages = convert_from_path(str(pdf_path), dpi=300)
        texts = []
        for page_img in pages:
            text = pytesseract.image_to_string(page_img, lang="eng")
            texts.append(text)
        return "\n".join(texts)


# Module-level singleton
ocr_service = OCRService()
