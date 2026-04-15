import datetime
import html
import asyncio
import hmac
import hashlib
import json
import os
import re
import smtplib
from base64 import b64encode
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Security, Request, BackgroundTasks, File, UploadFile, Form
from pydantic import BaseModel
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_session

from core.config import (
    API_KEY, api_key_header, APP_TITLE, SYDNEY_TZ, STOCK_ROOT, 
    PROJECT_ROOT, PROJECT_LOG_PATH, BRAND_NAME, BRAND_AREA, BRAND_LOGO_URL, 
    PRINCIPAL_NAME, PRINCIPAL_EMAIL, PRINCIPAL_PHONE, PROJECT_MEMORY_RULE, 
    BACKGROUND_SEND_POLL_SECONDS, PRIMARY_STRIKE_SUBURB, SECONDARY_STRIKE_SUBURBS,
    USE_POSTGRES
)
from core.utils import (
    now_sydney, now_iso, format_sydney, parse_client_datetime, 
    _first_non_empty, _safe_int, _format_moneyish, _parse_json_list, 
    _encode_value, _decode_row, _dedupe_text_list, _normalize_phone, 
    _dedupe_by_phone, _parse_iso_datetime, _parse_calendar_date, 
    _month_range_from_date, _bool_db
)
from services.scoring import _trigger_bonus, _status_penalty, _score_lead
from models.schemas import *
from core.logic import *

from services.automations import _schedule_task, _refresh_lead_next_action
from core.security import get_api_key

router = APIRouter()

@router.get("/api/cotality/account")
async def get_cotality_account(api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    try:
        res = await session.execute(text("SELECT * FROM cotality_accounts ORDER BY updated_at DESC LIMIT 1"))
        account = res.mappings().first()
    except Exception:
        account = None
        
    return dict(account) if account else {
        "label": "Primary Cotality",
        "api_base": "",
        "api_key": "",
        "property_path": "/property",
        "valuation_path": "/valuation",
        "comparables_path": "/comparables",
        "suburb_path": "/suburb",
        "rental_path": "/rental",
        "listing_path": "/listings",
        "market_path": "/market",
        "enabled": False,
    }


@router.post("/api/cotality/account")
async def save_cotality_account(body: CotalityAccount, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    account_id = body.id or hashlib.md5(f"{body.label}:{body.api_base}".encode()).hexdigest()
    now = now_iso()
    await session.execute(
        text("""
        INSERT INTO cotality_accounts (id, label, api_base, api_key, property_path, valuation_path, comparables_path, suburb_path,
            rental_path, listing_path, market_path, enabled, created_at, updated_at)
        VALUES (:id, :label, :api_base, :api_key, :property_path, :valuation_path, :comparables_path, :suburb_path,
            :rental_path, :listing_path, :market_path, :enabled, :created_at, :updated_at)
        ON CONFLICT(id) DO UPDATE SET
            label=EXCLUDED.label, api_base=EXCLUDED.api_base, api_key=EXCLUDED.api_key,
            property_path=EXCLUDED.property_path, valuation_path=EXCLUDED.valuation_path, comparables_path=EXCLUDED.comparables_path,
            suburb_path=EXCLUDED.suburb_path, rental_path=EXCLUDED.rental_path, listing_path=EXCLUDED.listing_path,
            market_path=EXCLUDED.market_path, enabled=EXCLUDED.enabled, updated_at=EXCLUDED.updated_at
        """),
        {
            "id": account_id,
            "label": body.label,
            "api_base": body.api_base,
            "api_key": body.api_key,
            "property_path": body.property_path,
            "valuation_path": body.valuation_path,
            "comparables_path": body.comparables_path,
            "suburb_path": body.suburb_path,
            "rental_path": body.rental_path,
            "listing_path": body.listing_path,
            "market_path": body.market_path,
            "enabled": 1 if body.enabled else 0,
            "created_at": now,
            "updated_at": now,
        },
    )
    await session.commit()
    return {"status": "ok", "id": account_id}


@router.get("/api/cotality/reports/{report_id}")
async def get_cotality_report(report_id: str, api_key: str = Depends(get_api_key), session: AsyncSession = Depends(get_session)):
    res = await session.execute(text("SELECT * FROM cotality_reports WHERE id = :id"), {"id": report_id})
    row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Report not found")
    report = dict(row)
    report["payload"] = json.loads(report.pop("json_payload"))
    return report
