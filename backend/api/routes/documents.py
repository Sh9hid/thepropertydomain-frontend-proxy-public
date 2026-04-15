from fastapi import APIRouter, HTTPException

from api.routes._deps import APIKeyDep, SessionDep
from services.document_service import generate_cma, generate_sales_advice, generate_seller_insight

router = APIRouter()


def _document_error(exc: Exception) -> HTTPException:
    message = str(exc)
    if message == "Lead not found":
        return HTTPException(status_code=404, detail=message)
    return HTTPException(status_code=400, detail=message)


@router.get("/api/documents/{lead_id}/sales-advice")
async def get_sales_advice_document(lead_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await generate_sales_advice(lead_id, session=session)
    except ValueError as exc:
        raise _document_error(exc) from exc


@router.get("/api/documents/{lead_id}/cma")
async def get_cma_document(lead_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await generate_cma(lead_id, session=session)
    except ValueError as exc:
        raise _document_error(exc) from exc


@router.get("/api/documents/{lead_id}/seller-insight")
async def get_seller_insight_document(lead_id: str, api_key: APIKeyDep, session: SessionDep):
    try:
        return await generate_seller_insight(lead_id, session=session)
    except ValueError as exc:
        raise _document_error(exc) from exc
