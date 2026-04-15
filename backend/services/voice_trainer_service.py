"""
Voice Trainer Service — post-call delivery analysis for outbound real estate calls.

Computes deterministic metrics from existing transcripts/segments/filler events,
checks sales structure, then generates AU-style coaching via Gemini.

No heavy ML. No fake scoring. Fast, repeatable, practical.
"""
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------
_FILLER_RE = re.compile(
    r"\b(um+|uh+|er+|ah+|like|you know|sort of|kind of|right|basically|"
    r"literally|actually|obviously|honestly|look|yeah nah|nah yeah)\b",
    re.IGNORECASE,
)
_INTRO_RE = re.compile(
    r"\b(my name is|i'?m (nitin|shahid)|calling from laing|this is (nitin|shahid)|"
    r"laing.{0,10}simmons)\b",
    re.IGNORECASE,
)
_REASON_RE = re.compile(
    r"\b(reason (i'?m|for my) call(ing)?|just (calling|ringing) (about|regarding)|"
    r"i (noticed|saw|was looking)|reason i called|quick (call|one)|"
    r"i'?m calling (about|because|to))\b",
    re.IGNORECASE,
)
_VALUE_RE = re.compile(
    r"\b(appraisal|market value|worth|recently sold|comparable|selling price|"
    r"buyers (are|we have)|interest in your (area|street|property)|"
    r"what your (place|property|home) (is worth|could fetch))\b",
    re.IGNORECASE,
)
_NEXT_STEP_RE = re.compile(
    r"\b(book|catch up|meet(ing)?|available|free (this|next)|when (would|are you)|"
    r"this week|next week|pop (around|over)|swing by|come (around|out)|"
    r"have a chat|quick chat|grab (a|some) time)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
async def ensure_voice_trainer_schema(session: AsyncSession) -> None:
    await session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS voice_trainer_reports (
                id TEXT PRIMARY KEY,
                call_id TEXT NOT NULL,
                lead_id TEXT,
                rep_id TEXT,
                pace_wpm REAL,
                filler_count INTEGER DEFAULT 0,
                filler_words TEXT,
                avg_pause_ms REAL,
                long_pause_count INTEGER DEFAULT 0,
                avg_sentence_words REAL,
                ramble_count INTEGER DEFAULT 0,
                agent_talk_ratio REAL,
                question_count INTEGER DEFAULT 0,
                intro_detected INTEGER DEFAULT 0,
                reason_stated INTEGER DEFAULT 0,
                value_prop_detected INTEGER DEFAULT 0,
                question_asked INTEGER DEFAULT 0,
                next_step_attempted INTEGER DEFAULT 0,
                highlights_json TEXT,
                issues_json TEXT,
                improved_opener TEXT,
                raw_issues_json TEXT,
                coaching_status TEXT DEFAULT 'pending',
                generated_at TEXT,
                created_at TEXT
            )
            """
        )
    )
    await session.execute(
        text("CREATE INDEX IF NOT EXISTS idx_vtr_call_id ON voice_trainer_reports(call_id)")
    )
    await session.execute(
        text("CREATE INDEX IF NOT EXISTS idx_vtr_lead_id ON voice_trainer_reports(lead_id)")
    )
    await session.commit()


# ---------------------------------------------------------------------------
# Deterministic metric computation
# ---------------------------------------------------------------------------
def _compute_metrics(
    segments: List[Dict[str, Any]],
    filler_rows: List[Dict[str, Any]],
    duration_seconds: int,
) -> Dict[str, Any]:
    agent_segs = [s for s in segments if s.get("speaker_role") == "agent"]

    # WPM — based on agent speech time only
    total_agent_words = sum(len(s["text"].split()) for s in agent_segs)
    agent_duration_ms = sum(
        max(0, s["end_ms"] - s["start_ms"]) for s in agent_segs if s.get("end_ms") and s.get("start_ms")
    )
    pace_wpm = (
        round((total_agent_words / agent_duration_ms) * 60000, 1) if agent_duration_ms > 500 else 0.0
    )

    # Fillers — prefer events table data, fall back to regex
    if filler_rows:
        filler_count = sum(int(r.get("count", 1)) for r in filler_rows)
        filler_words = list({(r.get("filler_text") or "").lower() for r in filler_rows if r.get("filler_text")})
    else:
        all_agent_text = " ".join(s["text"] for s in agent_segs)
        raw_matches = _FILLER_RE.findall(all_agent_text)
        filler_count = len(raw_matches)
        filler_words = list({(m if isinstance(m, str) else m[0]).lower() for m in raw_matches})

    # Pauses — gaps between consecutive segments (all speakers, ordered)
    sorted_segs = sorted(segments, key=lambda s: s.get("start_ms") or 0)
    gaps = []
    for i in range(1, len(sorted_segs)):
        gap = (sorted_segs[i].get("start_ms") or 0) - (sorted_segs[i - 1].get("end_ms") or 0)
        if gap > 300:
            gaps.append(gap)
    avg_pause_ms = round(sum(gaps) / len(gaps), 1) if gaps else 0.0
    long_pause_count = sum(1 for g in gaps if g > 2000)

    # Sentence length (agent only)
    sentence_word_counts = []
    for s in agent_segs:
        for sent in re.split(r"[.!?]+", s["text"]):
            wc = len(sent.split())
            if wc > 2:
                sentence_word_counts.append(wc)
    avg_sentence_words = (
        round(sum(sentence_word_counts) / len(sentence_word_counts), 1) if sentence_word_counts else 0.0
    )
    ramble_count = sum(1 for wc in sentence_word_counts if wc > 25)

    # Talk ratio
    total_ms = duration_seconds * 1000 if duration_seconds else 0
    agent_talk_ratio = round(agent_duration_ms / total_ms, 2) if total_ms > 0 else 0.0

    # Questions from agent text
    question_count = sum(s["text"].count("?") for s in agent_segs)

    return {
        "pace_wpm": pace_wpm,
        "filler_count": filler_count,
        "filler_words": filler_words,
        "avg_pause_ms": avg_pause_ms,
        "long_pause_count": long_pause_count,
        "avg_sentence_words": avg_sentence_words,
        "ramble_count": ramble_count,
        "agent_talk_ratio": agent_talk_ratio,
        "question_count": question_count,
    }


def _check_sales_structure(agent_segs: List[Dict[str, Any]]) -> Dict[str, bool]:
    early_text = " ".join(s["text"] for s in agent_segs[:4])
    all_text = " ".join(s["text"] for s in agent_segs)
    return {
        "intro_detected": bool(_INTRO_RE.search(early_text)),
        "reason_stated": bool(_REASON_RE.search(early_text)),
        "value_prop_detected": bool(_VALUE_RE.search(all_text)),
        "question_asked": "?" in all_text,
        "next_step_attempted": bool(_NEXT_STEP_RE.search(all_text)),
    }


def _build_raw_issues(metrics: Dict, structure: Dict) -> List[str]:
    issues = []
    pace = metrics.get("pace_wpm", 0)
    if pace > 170:
        issues.append(f"Speaking too fast ({pace:.0f} wpm) — aim for 130–160")
    elif 0 < pace < 110:
        issues.append(f"Speaking too slowly ({pace:.0f} wpm) — aim for 130–160")

    if metrics.get("filler_count", 0) > 5:
        issues.append(f"High filler count ({metrics['filler_count']}) — cut um/uh/like")
    if metrics.get("ramble_count", 0) > 1:
        issues.append(f"{metrics['ramble_count']} rambling sentence(s) — keep under 20 words per thought")
    if metrics.get("long_pause_count", 0) > 3:
        issues.append(f"{metrics['long_pause_count']} long pauses (>2s) — sounds hesitant or lost")
    if metrics.get("agent_talk_ratio", 0) > 0.75:
        issues.append("Talking >75% of the call — ask more, listen more")

    if not structure.get("intro_detected"):
        issues.append("No clear intro — state your name and agency upfront")
    if not structure.get("reason_stated"):
        issues.append("Reason for calling not stated early — homeowners hang up without this")
    if not structure.get("value_prop_detected"):
        issues.append("No value prop detected — mention market activity or appraisal offer")
    if not structure.get("next_step_attempted"):
        issues.append("No next step attempted — always go for the meeting")
    if metrics.get("question_count", 0) < 2:
        issues.append("Fewer than 2 questions asked — engage them with questions")

    return issues


# ---------------------------------------------------------------------------
# AI Coaching (Gemini — structured output)
# ---------------------------------------------------------------------------
_COACHING_SCHEMA = {
    "type": "object",
    "properties": {
        "highlights": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Exactly 3 specific things done well in this call",
        },
        "issues": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Exactly 3 highest-priority things to fix",
        },
        "improved_opener": {
            "type": "string",
            "description": "Rewritten first line — AU-casual, direct, under 30 words",
        },
    },
    "required": ["highlights", "issues", "improved_opener"],
}

_COACHING_SYSTEM = """\
You are a senior Australian real estate sales coach specialising in outbound vendor prospecting for suburban Sydney.

Your job: concise, specific, immediately actionable feedback. No fluff.

Australian delivery style:
- Relaxed, direct — not American pitch-y
- GOOD: "Hey, quick one — I was just looking at activity on your street and wanted to run something by you."
- BAD: "Hello! I'd love to take a moment of your time to share some exciting market insights with you today!"

Rules:
- Highlights: specific to THIS call — what actually went well, not generic praise
- Issues: top 3 highest-impact fixes ordered by priority
- Improved opener: rewrite the actual first line heard — AU-casual, under 30 words, no fake accent
- Never mention "accent" or "voice quality" — only structure, pacing, word choice, flow
"""


async def _generate_coaching(
    agent_text_excerpt: str,
    metrics: Dict,
    structure: Dict,
    raw_issues: List[str],
) -> Tuple[List[str], List[str], str]:
    try:
        import google.generativeai as genai
        from core.config import GEMINI_API_KEY

        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not configured")

        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash",
            system_instruction=_COACHING_SYSTEM,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json",
                response_schema=_COACHING_SCHEMA,
                temperature=0.4,
            ),
        )

        prompt = f"""Analyse this outbound real estate call and give specific, usable coaching.

AGENT TRANSCRIPT (first 400 words):
{agent_text_excerpt[:1800]}

METRICS:
- Pace: {metrics.get('pace_wpm', 0):.0f} wpm (target 130–160)
- Fillers (um/uh/like): {metrics.get('filler_count', 0)}
- Questions asked: {metrics.get('question_count', 0)}
- Agent talk ratio: {int(metrics.get('agent_talk_ratio', 0) * 100)}%
- Long pauses (>2s): {metrics.get('long_pause_count', 0)}
- Rambling sentences (>25w): {metrics.get('ramble_count', 0)}

STRUCTURE CHECK:
- Intro detected: {structure.get('intro_detected', False)}
- Reason for calling stated: {structure.get('reason_stated', False)}
- Value prop mentioned: {structure.get('value_prop_detected', False)}
- Question asked: {structure.get('question_asked', False)}
- Next step attempted: {structure.get('next_step_attempted', False)}

DETECTED ISSUES:
{chr(10).join(f'- {i}' for i in raw_issues) if raw_issues else '- None'}

Return exactly 3 highlights, 3 issues, and 1 improved opener."""

        response = model.generate_content(prompt)
        result = json.loads(response.text)
        highlights = (result.get("highlights") or [])[:3]
        issues = (result.get("issues") or [])[:3]
        improved_opener = result.get("improved_opener") or ""

        while len(highlights) < 3:
            highlights.append("Keep building call consistency")
        while len(issues) < 3:
            fallback = raw_issues[len(issues)] if len(raw_issues) > len(issues) else "Review call structure"
            issues.append(fallback)

        return highlights, issues, improved_opener

    except Exception as exc:
        log.warning("Voice trainer coaching (Gemini) failed: %s", exc)
        highlights = [
            "You completed the call — consistency is the foundation",
            "You opened a conversation — that's the hardest part done",
            "Keep dialling — volume builds confidence",
        ]
        issues = raw_issues[:3] if raw_issues else [
            "Review sales call structure (intro → reason → value → question → next step)",
            "Work on pacing — aim for 130–160 wpm",
            "Ask at least 2 questions per call",
        ]
        improved_opener = (
            "Hey, quick one — I was just looking at recent sales on your street and "
            "wanted to check in about your place."
        )
        return highlights, issues, improved_opener


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
async def run_voice_trainer(
    session: AsyncSession,
    call_id: str,
    lead_id: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    """
    Run voice trainer analysis for a call.
    Returns the full report payload dict.
    Cached — re-runs only if force=True.
    """
    existing = (
        await session.execute(
            text("SELECT * FROM voice_trainer_reports WHERE call_id = :call_id LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()

    if existing and not force:
        return _row_to_payload(dict(existing))

    # Load call metadata
    call_row = (
        await session.execute(
            text("SELECT id, lead_id, rep_id, duration_seconds FROM calls WHERE id = :call_id LIMIT 1"),
            {"call_id": call_id},
        )
    ).mappings().first()

    duration_seconds = int((call_row or {}).get("duration_seconds") or 0)
    rep_id = str((call_row or {}).get("rep_id") or "")
    if not lead_id and call_row:
        lead_id = str(call_row.get("lead_id") or "")

    # Load segments with speaker role via join
    segment_rows = (
        await session.execute(
            text(
                """
                SELECT cs.turn_index, cs.start_ms, cs.end_ms, cs.text,
                       COALESCE(sp.role, 'unknown') AS speaker_role
                FROM call_segments cs
                LEFT JOIN speakers sp ON sp.id = cs.speaker_id
                WHERE cs.call_id = :call_id
                ORDER BY cs.start_ms ASC, cs.turn_index ASC
                """
            ),
            {"call_id": call_id},
        )
    ).mappings().all()
    segments = [dict(r) for r in segment_rows]

    # Fallback: use full_text as a single agent segment
    if not segments:
        transcript_row = (
            await session.execute(
                text(
                    "SELECT full_text FROM transcripts "
                    "WHERE call_id = :call_id AND status = 'completed' LIMIT 1"
                ),
                {"call_id": call_id},
            )
        ).mappings().first()
        if transcript_row:
            full_text = str(transcript_row.get("full_text") or "")
            segments = [{
                "text": full_text,
                "start_ms": 0,
                "end_ms": duration_seconds * 1000,
                "speaker_role": "agent",
            }]
        else:
            return {"error": "no_transcript", "call_id": call_id}

    # Load filler events
    filler_rows = (
        await session.execute(
            text("SELECT filler_text, count FROM filler_events WHERE call_id = :call_id"),
            {"call_id": call_id},
        )
    ).mappings().all()

    # Compute
    metrics = _compute_metrics(segments, [dict(r) for r in filler_rows], duration_seconds)
    agent_segs = [s for s in segments if s.get("speaker_role") == "agent"]
    structure = _check_sales_structure(agent_segs)
    raw_issues = _build_raw_issues(metrics, structure)

    agent_text_excerpt = " ".join(s["text"] for s in agent_segs[:20])
    highlights, issues, improved_opener = await _generate_coaching(
        agent_text_excerpt, metrics, structure, raw_issues
    )

    # Persist
    now = datetime.now(timezone.utc).isoformat()
    report_id = str(existing["id"]) if existing else str(uuid.uuid4())
    params = _build_params(
        report_id, call_id, lead_id or "", rep_id,
        metrics, structure, highlights, issues, improved_opener, raw_issues, now
    )

    if existing:
        await session.execute(
            text(
                """
                UPDATE voice_trainer_reports SET
                    pace_wpm=:pace_wpm, filler_count=:filler_count, filler_words=:filler_words,
                    avg_pause_ms=:avg_pause_ms, long_pause_count=:long_pause_count,
                    avg_sentence_words=:avg_sentence_words, ramble_count=:ramble_count,
                    agent_talk_ratio=:agent_talk_ratio, question_count=:question_count,
                    intro_detected=:intro_detected, reason_stated=:reason_stated,
                    value_prop_detected=:value_prop_detected, question_asked=:question_asked,
                    next_step_attempted=:next_step_attempted,
                    highlights_json=:highlights_json, issues_json=:issues_json,
                    improved_opener=:improved_opener, raw_issues_json=:raw_issues_json,
                    coaching_status='completed', generated_at=:generated_at
                WHERE id=:id
                """
            ),
            params,
        )
    else:
        await session.execute(
            text(
                """
                INSERT INTO voice_trainer_reports (
                    id, call_id, lead_id, rep_id,
                    pace_wpm, filler_count, filler_words,
                    avg_pause_ms, long_pause_count, avg_sentence_words,
                    ramble_count, agent_talk_ratio, question_count,
                    intro_detected, reason_stated, value_prop_detected,
                    question_asked, next_step_attempted,
                    highlights_json, issues_json, improved_opener,
                    raw_issues_json, coaching_status, generated_at, created_at
                ) VALUES (
                    :id, :call_id, :lead_id, :rep_id,
                    :pace_wpm, :filler_count, :filler_words,
                    :avg_pause_ms, :long_pause_count, :avg_sentence_words,
                    :ramble_count, :agent_talk_ratio, :question_count,
                    :intro_detected, :reason_stated, :value_prop_detected,
                    :question_asked, :next_step_attempted,
                    :highlights_json, :issues_json, :improved_opener,
                    :raw_issues_json, 'completed', :generated_at, :generated_at
                )
                """
            ),
            params,
        )

    await session.commit()
    return _build_payload(
        report_id, call_id, lead_id, rep_id,
        metrics, structure, highlights, issues, improved_opener, raw_issues, now
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_params(
    report_id: str, call_id: str, lead_id: str, rep_id: str,
    metrics: Dict, structure: Dict,
    highlights: List, issues: List, improved_opener: str,
    raw_issues: List, now: str,
) -> Dict:
    return {
        "id": report_id,
        "call_id": call_id,
        "lead_id": lead_id,
        "rep_id": rep_id,
        "pace_wpm": metrics.get("pace_wpm"),
        "filler_count": metrics.get("filler_count", 0),
        "filler_words": json.dumps(metrics.get("filler_words", [])),
        "avg_pause_ms": metrics.get("avg_pause_ms"),
        "long_pause_count": metrics.get("long_pause_count", 0),
        "avg_sentence_words": metrics.get("avg_sentence_words"),
        "ramble_count": metrics.get("ramble_count", 0),
        "agent_talk_ratio": metrics.get("agent_talk_ratio"),
        "question_count": metrics.get("question_count", 0),
        "intro_detected": int(structure.get("intro_detected", False)),
        "reason_stated": int(structure.get("reason_stated", False)),
        "value_prop_detected": int(structure.get("value_prop_detected", False)),
        "question_asked": int(structure.get("question_asked", False)),
        "next_step_attempted": int(structure.get("next_step_attempted", False)),
        "highlights_json": json.dumps(highlights),
        "issues_json": json.dumps(issues),
        "improved_opener": improved_opener,
        "raw_issues_json": json.dumps(raw_issues),
        "generated_at": now,
    }


def _build_payload(
    report_id: str, call_id: str, lead_id: Optional[str], rep_id: str,
    metrics: Dict, structure: Dict,
    highlights: List, issues: List, improved_opener: str,
    raw_issues: List, now: str,
) -> Dict:
    return {
        "id": report_id,
        "call_id": call_id,
        "lead_id": lead_id,
        "rep_id": rep_id,
        "pace_wpm": metrics.get("pace_wpm", 0.0),
        "filler_count": metrics.get("filler_count", 0),
        "filler_words": metrics.get("filler_words", []),
        "avg_pause_ms": metrics.get("avg_pause_ms", 0.0),
        "long_pause_count": metrics.get("long_pause_count", 0),
        "avg_sentence_words": metrics.get("avg_sentence_words", 0.0),
        "ramble_count": metrics.get("ramble_count", 0),
        "agent_talk_ratio": metrics.get("agent_talk_ratio", 0.0),
        "question_count": metrics.get("question_count", 0),
        "intro_detected": structure.get("intro_detected", False),
        "reason_stated": structure.get("reason_stated", False),
        "value_prop_detected": structure.get("value_prop_detected", False),
        "question_asked": structure.get("question_asked", False),
        "next_step_attempted": structure.get("next_step_attempted", False),
        "highlights": highlights,
        "issues": issues,
        "improved_opener": improved_opener,
        "raw_issues": raw_issues,
        "coaching_status": "completed",
        "generated_at": now,
    }


def _safe_json(val: Any, default: Any) -> Any:
    if val is None:
        return default
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return default


def _row_to_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": row.get("id"),
        "call_id": row.get("call_id"),
        "lead_id": row.get("lead_id"),
        "rep_id": row.get("rep_id"),
        "pace_wpm": row.get("pace_wpm", 0.0),
        "filler_count": row.get("filler_count", 0),
        "filler_words": _safe_json(row.get("filler_words"), []),
        "avg_pause_ms": row.get("avg_pause_ms", 0.0),
        "long_pause_count": row.get("long_pause_count", 0),
        "avg_sentence_words": row.get("avg_sentence_words", 0.0),
        "ramble_count": row.get("ramble_count", 0),
        "agent_talk_ratio": row.get("agent_talk_ratio", 0.0),
        "question_count": row.get("question_count", 0),
        "intro_detected": bool(row.get("intro_detected")),
        "reason_stated": bool(row.get("reason_stated")),
        "value_prop_detected": bool(row.get("value_prop_detected")),
        "question_asked": bool(row.get("question_asked")),
        "next_step_attempted": bool(row.get("next_step_attempted")),
        "highlights": _safe_json(row.get("highlights_json"), []),
        "issues": _safe_json(row.get("issues_json"), []),
        "improved_opener": row.get("improved_opener") or "",
        "raw_issues": _safe_json(row.get("raw_issues_json"), []),
        "coaching_status": row.get("coaching_status") or "pending",
        "generated_at": row.get("generated_at"),
    }
