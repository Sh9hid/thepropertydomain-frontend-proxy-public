"""
Visual Deduplication via CLIP ViT-B/32 INT8 ONNX embeddings.
512-dim embeddings stored in intelligence.media.image_embedding (pgvector).
Cosine distance ≤ 0.12 = duplicate.

Pre-requisite: backend/models/clip_quantized.onnx must be generated offline.
See backend/models/README_CLIP.md for export instructions.
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

CLIP_MODEL_PATH = Path(__file__).parents[1] / "models" / "clip_quantized.onnx"
SIMILARITY_THRESHOLD = 0.12  # cosine distance; lower = more similar


try:
    import onnxruntime as ort
    HAS_ORT = True
except ImportError:
    ort = None
    HAS_ORT = False
    logger.warning("[CLIP] onnxruntime not installed — visual dedup disabled")


class CLIPDedup:
    """
    CLIP ViT-B/32 exported to ONNX INT8 (~153 MB).
    Runs on CPU or CUDA (GTX 1650 4 GB VRAM).
    512-dim embeddings stored in intelligence.media.image_embedding vector(512).
    """

    def __init__(self):
        self._session = None
        if HAS_ORT and CLIP_MODEL_PATH.exists():
            try:
                providers = ["CUDAExecutionProvider", "CPUExecutionProvider"]
                self._session = ort.InferenceSession(
                    str(CLIP_MODEL_PATH), providers=providers
                )
                logger.info(f"[CLIP] ONNX model loaded from {CLIP_MODEL_PATH}")
            except Exception as e:
                logger.warning(f"[CLIP] Failed to load ONNX model: {e}")
        elif not CLIP_MODEL_PATH.exists():
            logger.warning(
                f"[CLIP] Model file not found at {CLIP_MODEL_PATH}. "
                "Run the export script in backend/models/README_CLIP.md first."
            )

    def embed_image_url(self, image_url: str) -> Optional[np.ndarray]:
        """Download image → preprocess → ONNX inference → 512-dim float32 vector."""
        if self._session is None:
            return None
        try:
            import httpx
            from PIL import Image
            import io

            resp = httpx.get(image_url, timeout=10)
            resp.raise_for_status()
            img = Image.open(io.BytesIO(resp.content)).convert("RGB")
            img = img.resize((224, 224))

            # Normalise: ImageNet mean/std
            arr = np.array(img, dtype=np.float32) / 255.0
            mean = np.array([0.48145466, 0.4578275, 0.40821073], dtype=np.float32)
            std = np.array([0.26862954, 0.26130258, 0.27577711], dtype=np.float32)
            arr = (arr - mean) / std
            arr = arr.transpose(2, 0, 1)[np.newaxis]  # (1, 3, 224, 224)

            input_name = self._session.get_inputs()[0].name
            outputs = self._session.run(None, {input_name: arr})
            embedding = outputs[0][0]  # (512,)

            # L2 normalise
            norm = np.linalg.norm(embedding)
            if norm > 0:
                embedding = embedding / norm
            return embedding.astype(np.float32)
        except Exception as e:
            logger.warning(f"[CLIP] embed_image_url error ({image_url}): {e}")
            return None

    async def find_visual_duplicate(
        self,
        db,
        embedding: np.ndarray,
        exclude_property_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Query intelligence.media for nearest neighbour via pgvector cosine distance.
        Returns property_id if cosine distance < SIMILARITY_THRESHOLD, else None.
        """
        if embedding is None:
            return None
        try:
            from sqlalchemy import text
            # Encode embedding as Postgres vector literal
            vec_str = "[" + ",".join(f"{v:.6f}" for v in embedding.tolist()) + "]"
            sql = """
                SELECT property_id,
                       image_embedding <=> :embedding::vector AS distance
                FROM intelligence.media
                WHERE image_embedding IS NOT NULL
            """
            params: dict = {"embedding": vec_str}
            if exclude_property_id:
                sql += " AND property_id != :exclude_id"
                params["exclude_id"] = exclude_property_id
            sql += " ORDER BY distance LIMIT 1"

            result = await db.execute(text(sql), params)
            row = result.fetchone()
            if row and row[1] < SIMILARITY_THRESHOLD:
                return row[0]
            return None
        except Exception as e:
            logger.warning(f"[CLIP] find_visual_duplicate error: {e}")
            return None


# Module-level singleton (lazy ONNX load)
clip_dedup = CLIPDedup()
