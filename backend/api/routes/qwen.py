from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from services.qwen_service import ask_qwen, QwenResponse
from core.security import get_api_key

router = APIRouter(tags=["qwen"])

class QwenQuery(BaseModel):
    query: str

@router.post("/api/qwen/ask")
async def ask_qwen_route(body: QwenQuery, api_key: str = Depends(get_api_key)) -> QwenResponse:
    """
    Direct interface to QWEN (NVIDIA NIM Qwen 3.5 397B).
    """
    try:
        return await ask_qwen(body.query)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
