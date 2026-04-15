# CLIP ViT-B/32 ONNX Model — One-Time Setup

## What This Is

The visual deduplication engine (`services/dedup_visual.py`) uses a quantised
CLIP ViT-B/32 model exported to ONNX INT8 format (~153 MB).

The file `backend/models/clip_quantized.onnx` is **not included in the repo**
(too large for git). You must generate it once before the Docker build.

## Prerequisites

```bash
pip install transformers torch onnx onnxruntime
```

## Export + Quantise Script

Run this from the repo root (or any machine with enough RAM/disk):

```python
import torch
from pathlib import Path
from transformers import CLIPModel, CLIPProcessor

# 1. Load model
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
model.eval()

# 2. Export vision encoder to ONNX (FP32)
dummy_input = torch.randn(1, 3, 224, 224)
onnx_fp32_path = "backend/models/clip_fp32.onnx"

torch.onnx.export(
    model.vision_model,
    dummy_input,
    onnx_fp32_path,
    input_names=["pixel_values"],
    output_names=["last_hidden_state", "pooler_output"],
    dynamic_axes={"pixel_values": {0: "batch_size"}},
    opset_version=14,
)
print(f"Exported FP32 ONNX to {onnx_fp32_path}")

# 3. Quantise to INT8
from onnxruntime.quantization import quantize_dynamic, QuantType

quantize_dynamic(
    onnx_fp32_path,
    "backend/models/clip_quantized.onnx",
    weight_type=QuantType.QInt8,
)
print("Quantised model saved to backend/models/clip_quantized.onnx")
```

## Verify

```bash
python -c "
import onnxruntime as ort
s = ort.InferenceSession('backend/models/clip_quantized.onnx')
print('Inputs:', [i.name for i in s.get_inputs()])
print('OK — model loaded')
"
```

## Docker Build Note

`clip_quantized.onnx` is excluded from `.dockerignore` by default so it IS
copied into the container image. If the file is absent, `dedup_visual.py` will
log a warning and gracefully disable visual dedup — all other features continue
to work normally.

## GPU Acceleration

ONNX Runtime will auto-select `CUDAExecutionProvider` if a CUDA GPU is present
(GTX 1650 4 GB VRAM is sufficient). Falls back to CPU otherwise.
