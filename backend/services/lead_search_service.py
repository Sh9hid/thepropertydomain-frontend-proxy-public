from __future__ import annotations

import hashlib
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.logic import _hydrate_lead
from core.utils import _decode_row


def _embed_text_deterministic(value: str, dim: int = 256) -> list[float]:
    """
    Deterministic embedding fallback.
    Uses SHA-256 stream expansion so query/document vectors are stable without
    external embedding providers.
    """
    seed = (value or "").strip().lower().encode("utf-8")
    if not seed:
        return [0.0] * dim
    out: list[float] = []
    counter = 0
    while len(out) < dim:
        digest = hashlib.sha256(seed + b"|" + str(counter).encode("ascii")).digest()
        for b in digest:
            out.append((float(b) / 127.5) - 1.0)
            if len(out) >= dim:
                break
        counter += 1
    return out


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in values) + "]"


def _lead_search_text(row: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in (
        "address",
        "owner_name",
        "suburb",
        "canonical_address",
        "postcode",
        "trigger_type",
        "status",
        "scenario",
        "why_now",
        "what_to_say",
        "recommended_next_step",
    ):
        value = row.get(key)
        if value:
            chunks.append(str(value))
    for key in ("source_tags", "key_details", "summary_points", "risk_flags"):
        value = row.get(key)
        if isinstance(value, list) and value:
            chunks.extend(str(v) for v in value if v)
    for key in ("contact_phones", "contact_emails", "alternate_phones", "alternate_emails"):
        value = row.get(key)
        if isinstance(value, list) and value:
            chunks.extend(str(v) for v in value if v)
    return " | ".join(chunks)


async def _is_postgres(session: AsyncSession) -> bool:
    bind = session.bind
    return bool(bind and not str(bind.url).startswith("sqlite"))


async def refresh_lead_search_index(session: AsyncSession, *, batch_size: int = 3000) -> dict[str, Any]:
    """
    Rebuild or upsert lead search index rows.
    """
    if not await _is_postgres(session):
        return {"indexed": 0, "vector_enabled": False, "backend": "sqlite"}

    rows = (
        await session.execute(
            text(
                """
                SELECT id, address, owner_name, suburb, postcode, trigger_type, status,
                       canonical_address, scenario, why_now, what_to_say, recommended_next_step,
                       contact_phones, contact_emails, alternate_phones, alternate_emails,
                       source_tags, key_details, summary_points, risk_flags
                FROM leads
                ORDER BY COALESCE(updated_at, created_at, '') DESC
                LIMIT :limit
                """
            ),
            {"limit": int(batch_size)},
        )
    ).mappings().all()

    vector_enabled = False
    try:
        check = await session.execute(
            text(
                "SELECT 1 FROM pg_extension WHERE extname = 'vector' LIMIT 1"
            )
        )
        vector_enabled = bool(check.scalar_one_or_none())
    except Exception:
        vector_enabled = False

    indexed = 0
    for row in rows:
        payload = dict(row)
        lead_id = str(payload.get("id") or "").strip()
        if not lead_id:
            continue
        search_text = _lead_search_text(payload)
        emb = _embed_text_deterministic(search_text)
        params = {
            "lead_id": lead_id,
            "search_text": search_text,
            "embedding": _vector_literal(emb),
        }
        if vector_enabled:
            await session.execute(
                text(
                    """
                    INSERT INTO lead_search_index (lead_id, search_text, embedding, updated_at)
                    VALUES (:lead_id, :search_text, CAST(:embedding AS vector), NOW())
                    ON CONFLICT (lead_id)
                    DO UPDATE SET
                        search_text = EXCLUDED.search_text,
                        embedding = EXCLUDED.embedding,
                        updated_at = NOW()
                    """
                ),
                params,
            )
        else:
            await session.execute(
                text(
                    """
                    INSERT INTO lead_search_index (lead_id, search_text, updated_at)
                    VALUES (:lead_id, :search_text, NOW())
                    ON CONFLICT (lead_id)
                    DO UPDATE SET
                        search_text = EXCLUDED.search_text,
                        updated_at = NOW()
                    """
                ),
                params,
            )
        indexed += 1
    await session.commit()
    return {"indexed": indexed, "vector_enabled": vector_enabled, "backend": "postgres"}


async def search_leads_hybrid(
    session: AsyncSession,
    *,
    q: str,
    limit: int = 50,
    signal_status: str | None = None,
) -> dict[str, Any]:
    query = (q or "").strip()
    if not query:
        return {"leads": [], "total": 0, "vector_enabled": False, "mode": "empty_query"}

    limit = max(1, min(int(limit or 50), 200))
    normalized_signal_status = (signal_status or "").strip().upper()
    digit_query = "".join(ch for ch in query if ch.isdigit())

    if not await _is_postgres(session):
        like = f"%{query.lower()}%"
        rows = (
            await session.execute(
                text(
                    """
                    SELECT *
                    FROM leads
                    WHERE (
                        LOWER(COALESCE(address, '')) LIKE :q
                        OR LOWER(COALESCE(owner_name, '')) LIKE :q
                        OR LOWER(COALESCE(suburb, '')) LIKE :q
                        OR LOWER(COALESCE(canonical_address, '')) LIKE :q
                        OR LOWER(COALESCE(postcode, '')) LIKE :q
                        OR LOWER(COALESCE(CAST(contact_phones AS TEXT), '')) LIKE :q
                        OR LOWER(COALESCE(CAST(contact_emails AS TEXT), '')) LIKE :q
                    )
                    ORDER BY COALESCE(updated_at, created_at, '') DESC
                    LIMIT :limit
                    """
                ),
                {"q": like, "limit": limit},
            )
        ).mappings().all()
        leads = [_hydrate_lead(_decode_row(dict(row))) for row in rows]
        return {"leads": leads, "total": len(leads), "vector_enabled": False, "mode": "sqlite_like"}

    where_parts = []
    params: dict[str, Any] = {
        "q": query,
        "q_like": f"%{query.lower()}%",
        "digits_like": f"%{digit_query}%" if digit_query else "",
        "topk": max(100, limit * 4),
        "limit": limit,
    }
    if normalized_signal_status:
        where_parts.append("UPPER(COALESCE(l.signal_status, '')) = :signal_status")
        params["signal_status"] = normalized_signal_status
    where_sql = f" AND {' AND '.join(where_parts)}" if where_parts else ""

    lexical_rows = (
        await session.execute(
            text(
                f"""
                SELECT
                    l.id AS lead_id,
                    (
                        ts_rank_cd(
                            to_tsvector('simple',
                                COALESCE(l.address, '') || ' ' ||
                                COALESCE(l.owner_name, '') || ' ' ||
                                COALESCE(l.suburb, '') || ' ' ||
                                COALESCE(l.canonical_address, '') || ' ' ||
                                COALESCE(l.postcode, '') || ' ' ||
                                COALESCE(CAST(l.contact_phones AS TEXT), '') || ' ' ||
                                COALESCE(CAST(l.contact_emails AS TEXT), '') || ' ' ||
                                COALESCE(l.trigger_type, '')
                            ),
                            plainto_tsquery('simple', :q)
                        )
                        + similarity(
                            COALESCE(l.address, '') || ' ' ||
                            COALESCE(l.owner_name, '') || ' ' ||
                            COALESCE(l.suburb, '') || ' ' ||
                            COALESCE(l.canonical_address, '') || ' ' ||
                            COALESCE(l.postcode, '') || ' ' ||
                            COALESCE(CAST(l.contact_phones AS TEXT), '') || ' ' ||
                            COALESCE(CAST(l.contact_emails AS TEXT), ''),
                            :q
                        )
                    ) AS lexical_score
                FROM leads l
                WHERE (
                    to_tsvector('simple',
                        COALESCE(l.address, '') || ' ' ||
                        COALESCE(l.owner_name, '') || ' ' ||
                        COALESCE(l.suburb, '') || ' ' ||
                        COALESCE(l.canonical_address, '') || ' ' ||
                        COALESCE(l.postcode, '') || ' ' ||
                        COALESCE(CAST(l.contact_phones AS TEXT), '') || ' ' ||
                        COALESCE(CAST(l.contact_emails AS TEXT), '') || ' ' ||
                        COALESCE(l.trigger_type, '')
                    ) @@ plainto_tsquery('simple', :q)
                    OR LOWER(
                        COALESCE(l.address, '') || ' ' ||
                        COALESCE(l.owner_name, '') || ' ' ||
                        COALESCE(l.suburb, '') || ' ' ||
                        COALESCE(l.canonical_address, '') || ' ' ||
                        COALESCE(l.postcode, '') || ' ' ||
                        COALESCE(CAST(l.contact_phones AS TEXT), '') || ' ' ||
                        COALESCE(CAST(l.contact_emails AS TEXT), '')
                    ) LIKE :q_like
                    OR (
                        :digits_like <> ''
                        AND REGEXP_REPLACE(
                            COALESCE(l.address, '') || ' ' ||
                            COALESCE(l.canonical_address, '') || ' ' ||
                            COALESCE(l.postcode, '') || ' ' ||
                            COALESCE(CAST(l.contact_phones AS TEXT), '') || ' ' ||
                            COALESCE(CAST(l.contact_emails AS TEXT), ''),
                            '[^0-9]',
                            '',
                            'g'
                        ) LIKE :digits_like
                    )
                ){where_sql}
                ORDER BY lexical_score DESC
                LIMIT :topk
                """
            ),
            params,
        )
    ).mappings().all()

    vector_enabled = False
    vector_rows: list[dict[str, Any]] = []
    try:
        ext = await session.execute(text("SELECT 1 FROM pg_extension WHERE extname = 'vector' LIMIT 1"))
        vector_enabled = bool(ext.scalar_one_or_none())
    except Exception:
        vector_enabled = False

    if vector_enabled:
        qvec = _vector_literal(_embed_text_deterministic(query))
        try:
            vector_rows = (
                await session.execute(
                    text(
                        """
                        SELECT
                            s.lead_id,
                            (1.0 - (s.embedding <=> CAST(:qvec AS vector))) AS vector_score
                        FROM lead_search_index s
                        JOIN leads l ON l.id = s.lead_id
                        WHERE s.embedding IS NOT NULL
                        ORDER BY s.embedding <=> CAST(:qvec AS vector)
                        LIMIT :topk
                        """
                    ),
                    {"qvec": qvec, "topk": params["topk"]},
                )
            ).mappings().all()
        except Exception:
            vector_rows = []
            vector_enabled = False

    rrf_k = 60.0
    combined: dict[str, float] = {}
    for idx, row in enumerate(lexical_rows):
        lead_id = str(row["lead_id"])
        combined[lead_id] = combined.get(lead_id, 0.0) + 1.0 / (rrf_k + idx + 1)
    for idx, row in enumerate(vector_rows):
        lead_id = str(row["lead_id"])
        combined[lead_id] = combined.get(lead_id, 0.0) + 1.0 / (rrf_k + idx + 1)

    ranked_ids = [lead_id for lead_id, _ in sorted(combined.items(), key=lambda kv: kv[1], reverse=True)[:limit]]
    if not ranked_ids:
        return {"leads": [], "total": 0, "vector_enabled": vector_enabled, "mode": "hybrid_rrf"}

    detail_rows = (
        await session.execute(
            text("SELECT * FROM leads WHERE id = ANY(:ids)"),
            {"ids": ranked_ids},
        )
    ).mappings().all()
    by_id = {str(row["id"]): _hydrate_lead(_decode_row(dict(row))) for row in detail_rows}
    leads = [by_id[lead_id] for lead_id in ranked_ids if lead_id in by_id]
    return {
        "leads": leads,
        "total": len(leads),
        "vector_enabled": vector_enabled,
        "mode": "hybrid_rrf",
    }
