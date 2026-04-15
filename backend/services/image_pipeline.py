"""
Image Pipeline — property image deduplication and CLIP semantic embeddings.
Uses imagehash (perceptual hash) for fast deduplication and ONNX CLIP ViT-B/32
for semantic embeddings that can be matched to lead records.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import text

logger = logging.getLogger(__name__)

CLIP_MODEL_PATH = Path(os.getenv(
    "CLIP_MODEL_PATH",
    str(Path(__file__).resolve().parents[1] / "models" / "clip_vitb32_int8.onnx"),
))


class ImagePipeline:
    """
    Processes property images from STOCK_ROOT:
      - Computes perceptual hashes for deduplication
      - Generates CLIP embeddings for semantic matching
      - Associates images with lead records via address from file path
    """

    def __init__(self) -> None:
        self._ort_session = None

    def _get_ort_session(self):
        if self._ort_session is None and CLIP_MODEL_PATH.exists():
            try:
                import onnxruntime as ort
                self._ort_session = ort.InferenceSession(str(CLIP_MODEL_PATH))
            except Exception as exc:
                logger.warning(f"[ImagePipeline] Could not load CLIP model: {exc}")
        return self._ort_session

    def compute_phash(self, image_path: Path) -> Optional[str]:
        """Return perceptual hash string for deduplication."""
        try:
            import imagehash
            from PIL import Image
            img = Image.open(image_path)
            return str(imagehash.phash(img))
        except Exception as exc:
            logger.debug(f"[ImagePipeline] phash failed for {image_path.name}: {exc}")
            return None

    def compute_clip_embedding(self, image_path: Path) -> Optional[List[float]]:
        """Return 512-dim CLIP embedding as list of floats, or None if unavailable."""
        session = self._get_ort_session()
        if session is None:
            return None
        try:
            import numpy as np
            from PIL import Image

            img = Image.open(image_path).convert("RGB").resize((224, 224))
            arr = np.array(img, dtype=np.float32) / 255.0
            # Normalise using CLIP mean/std
            mean = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
            std = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)
            arr = (arr - mean) / std
            arr = arr.transpose(2, 0, 1)[np.newaxis]  # NCHW
            outputs = session.run(None, {"image": arr})
            embedding = outputs[0][0].tolist()
            return embedding
        except Exception as exc:
            logger.debug(f"[ImagePipeline] CLIP embed failed: {exc}")
            return None

    def walk_stock_root(self, stock_root: Optional[Path] = None) -> List[Dict[str, Any]]:
        """
        Walk STOCK_ROOT and return records of {path, phash, address_hint} for each image.
        Address hint is extracted from directory name (e.g. "14 Harvest Circuit Box Hill").
        """
        from core.config import STOCK_ROOT as DEFAULT_ROOT
        root = stock_root or DEFAULT_ROOT
        root = Path(root)
        if not root.exists():
            logger.warning(f"[ImagePipeline] STOCK_ROOT not found: {root}")
            return []

        records = []
        for img_path in root.rglob("*.jpg"):
            records.append(self._process_image(img_path))
        for img_path in root.rglob("*.png"):
            records.append(self._process_image(img_path))
        return [r for r in records if r]

    def _process_image(self, img_path: Path) -> Optional[Dict[str, Any]]:
        phash = self.compute_phash(img_path)
        if not phash:
            return None
        # Address hint from parent directory name
        address_hint = img_path.parent.name
        return {
            "path": str(img_path),
            "phash": phash,
            "address_hint": address_hint,
            "file_name": img_path.name,
        }

    async def associate_images_to_leads(self, records: List[Dict[str, Any]]) -> int:
        """
        Match image records to leads by address similarity and update main_image.
        Returns count of leads updated.
        """
        from core.database import async_engine
        from core.utils import now_iso
        from core.logic import _normalize_token
        from sqlalchemy.ext.asyncio import AsyncSession
        from sqlalchemy.orm import sessionmaker

        async_session = sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
        updated = 0
        async with async_session() as session:
            for rec in records:
                hint = _normalize_token(rec.get("address_hint", ""))
                if not hint:
                    continue
                res = await session.execute(
                    text("SELECT id, main_image FROM leads WHERE LOWER(REPLACE(address,' ','')) LIKE :hint"),
                    {"hint": f"%{hint.replace(' ', '')}%"}
                )
                row = res.mappings().first()
                if row and not row["main_image"]:
                    await session.execute(
                        text("UPDATE leads SET main_image = :path, updated_at = :upd WHERE id = :id"),
                        {"path": rec["path"], "upd": now_iso(), "id": row["id"]}
                    )
                    updated += 1
            await session.commit()
        return updated


# Module-level singleton
image_pipeline = ImagePipeline()
