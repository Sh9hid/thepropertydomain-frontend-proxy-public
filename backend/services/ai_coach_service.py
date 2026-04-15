"""AI Coach Service — Australian sales speech coaching engine."""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Australian speech coaching knowledge base
ACCENT_CORRECTIONS = {
    "today": {"wrong": "to-day", "correct": "t'daay", "note": "Flatten the 'o', let 'day' glide naturally — Aussie vowel shift"},
    "no": {"wrong": "no (flat)", "correct": "naow", "note": "Slight diphthong — let it round out naturally"},
    "going": {"wrong": "go-ing", "correct": "go-in", "note": "Drop the 'g' in casual speech — sounds more natural"},
    "data": {"wrong": "da-ta (American)", "correct": "dah-ta", "note": "Broad 'a' — Australian standard"},
    "can't": {"wrong": "cant (American)", "correct": "cahnt", "note": "Long 'a' sound — key Aussie marker"},
    "schedule": {"wrong": "sked-yool", "correct": "shed-yool", "note": "British-Australian pronunciation"},
    "property": {"wrong": "prop-er-tee", "correct": "prop-uh-tee", "note": "Soften the middle vowel"},
    "definitely": {"wrong": "def-in-ite-lee", "correct": "def-nuh-lee", "note": "Natural compression — don't over-enunciate"},
    "actually": {"wrong": "ak-chew-ally", "correct": "ak-shlee", "note": "Casual compression sounds more natural"},
    "good morning": {"wrong": "good morning (formal)", "correct": "g'morning / mornin'", "note": "Warm casual opener"},
    "how are you": {"wrong": "how are you (robotic)", "correct": "how ya goin'", "note": "Classic Aussie greeting — warm and relaxed"},
}

VOCAL_EXERCISES = [
    {"name": "Wide Palate Warm-Up", "instruction": "Open your mouth as wide as comfortable. Say 'AH-EE-OO' slowly 5 times. This opens your resonance chamber for a fuller Australian sound.", "duration_seconds": 30, "category": "warmup"},
    {"name": "Jaw Release", "instruction": "Place your fist under your chin. Open your mouth against gentle resistance. Hold 5 seconds, release. Repeat 5 times. This relaxes tension that makes speech sound tight.", "duration_seconds": 30, "category": "warmup"},
    {"name": "Smile While Speaking", "instruction": "Read any sentence while maintaining a slight smile. Notice how it changes your tone — warmer, more inviting. Practice 'How ya goin'?' with a smile. This is your default phone voice.", "duration_seconds": 20, "category": "warmth"},
    {"name": "Falling Tone Certainty", "instruction": "Say 'I can definitely help you with that' letting your pitch DROP on the last word. Falling tone = certainty. Rising tone = uncertainty. Sales requires certainty.", "duration_seconds": 15, "category": "confidence"},
    {"name": "Breath Reset", "instruction": "Before a high-stakes line (price, close, objection response), take one deliberate breath through the nose. This prevents rushed delivery and projects calm authority.", "duration_seconds": 10, "category": "confidence"},
    {"name": "Australian Diphthong Drill", "instruction": "Practice: 'No way, mate' — let 'no' become 'naow', 'way' become 'waay', 'mate' become 'mayt'. Exaggerate slightly, then dial back to 70%. That's your natural zone.", "duration_seconds": 20, "category": "accent"},
    {"name": "Lip Spread vs Round", "instruction": "Say 'fleece' (spread lips) then 'goose' (round lips). Alternate 10 times quickly. Australian English uses more lip spread than American. This muscle memory helps.", "duration_seconds": 20, "category": "accent"},
    {"name": "Tongue Placement", "instruction": "For the Australian 'r' — your tongue should NOT curl back (that's American). Keep it flat, barely touching. Say 'car park' — notice the 'r' almost disappears. That's correct.", "duration_seconds": 15, "category": "accent"},
]

HOSTILE_RESPONSE_TEMPLATES = {
    "fuck_off": [
        "No worries at all — sorry to catch you at a bad time.",
        "All good — I'll leave you in peace. Have a good one.",
        "Fair enough — thanks anyway, take care.",
    ],
    "not_interested": [
        "Completely understand — if anything changes, we're just around the corner in Oakville.",
        "No stress at all. Mind if I ask — is it the timing or have you already got someone helping?",
        "All good — just wanted to make sure you knew about [specific market data point]. Have a great day.",
    ],
    "already_have_agent": [
        "Oh great — good to hear you're sorted. If you ever want a second opinion on value, we're always happy to help.",
        "No worries — hope they're looking after you well. We're always here if you need a fresh set of eyes.",
        "That's awesome. If you're ever curious how things are tracking in the area, feel free to give us a ring.",
    ],
    "angry_general": [
        "I really appreciate your honesty — I'll make a note not to bother you again.",
        "Understood. Sorry for the interruption. Have a good day.",
        "You're right, and I apologise for the call. Take care.",
    ],
}

ISSUE_TYPES = {
    "accent": "Pronunciation / Australian accent naturalness",
    "wording": "Word choice — more natural/effective alternatives",
    "confidence": "Confidence drop — voice thinning, pace rushing, hesitation",
    "objection": "Objection handling — missed or weak recovery",
    "warmth": "Warmth / friendliness — sounding mechanical or cold",
    "opener": "Opening line — weak or robotic start",
    "close": "Closing / booking ask — missed or fumbled",
    "recovery": "Recovery after awkward/hostile moment",
    "pacing": "Pacing — too fast, too slow, or uneven",
    "filler": "Filler words — um, uh, like, you know, sort of",
    "inflection": "Upward inflection where certainty needed",
}


async def generate_coaching_session(
    call_id: int,
    transcript_text: str,
    segments: list,
    user_id: str = "shahid",
    existing_habits: list = None,
) -> dict:
    """
    Generate a full coaching session for a call.

    Args:
        call_id: The call log ID
        transcript_text: Full transcript text
        segments: List of diarized segments [{speaker, text, start_ms, end_ms}]
        user_id: Operator ID
        existing_habits: List of existing SpeechHabit dicts for recurrence detection

    Returns:
        Dict with coaching_events, scores, habit_updates, exercises
    """
    from services.ai_router import ask

    existing_habits = existing_habits or []
    habits_context = ""
    if existing_habits:
        habits_context = "\n\nKNOWN RECURRING ISSUES FOR THIS USER:\n"
        for h in existing_habits[:20]:
            habits_context += (
                f"- {h['habit_type']}: {h['description']} "
                f"(seen {h['occurrence_count']}x, mastery: {h['mastery_state']})\n"
            )

    accent_ref = "\n\nAUSTRALIAN PRONUNCIATION REFERENCE:\n"
    for word, info in ACCENT_CORRECTIONS.items():
        accent_ref += f"- '{word}': Say '{info['correct']}' not '{info['wrong']}' — {info['note']}\n"

    # Build segment text for analysis
    segment_text = ""
    for seg in segments[:100]:
        speaker = seg.get("speaker", seg.get("speaker_role", "unknown"))
        text = seg.get("text", "")
        start = seg.get("start_ms", 0)
        end = seg.get("end_ms", 0)
        segment_text += f"[{start}ms-{end}ms] {speaker}: {text}\n"

    if not segment_text.strip():
        segment_text = transcript_text

    prompt = f"""You are an expert Australian sales speech coach analyzing a real estate cold call.
The caller works for Laing+Simmons Oakville | Windsor in Western Sydney.

ANALYZE THIS CALL TRANSCRIPT AND PRODUCE COACHING EVENTS.
{habits_context}
{accent_ref}

TRANSCRIPT WITH TIMESTAMPS:
{segment_text}

For EVERY coachable moment, produce a JSON object. Find at least 5 moments, up to 20.
Focus on:
1. ACCENT: Words pronounced non-Australian, opportunities to sound more natural
2. WORDING: Robotic/formal phrases that could be warmer/more natural
3. CONFIDENCE: Moments where voice likely dropped, rushed, or hesitated
4. OBJECTION HANDLING: Weak or missed responses to resistance
5. WARMTH: Cold/mechanical moments that needed friendliness
6. OPENER: Was the opening warm and engaging?
7. CLOSE: Was there a clear, confident booking ask?
8. RECOVERY: After awkward moments — was recovery smooth?
9. PACING: Too fast (nervous) or too slow (losing energy)?
10. FILLER: Excessive um/uh/like/you know usage
11. INFLECTION: Upward inflection where downward certainty was needed

Output ONLY a JSON object with this exact structure:
{{
  "coaching_events": [
    {{
      "timestamp_start_ms": 0,
      "timestamp_end_ms": 5000,
      "subtitle_excerpt": "exact quote from transcript",
      "issue_type": "accent|wording|confidence|objection|warmth|opener|close|recovery|pacing|filler|inflection",
      "severity": "minor|moderate|critical",
      "what_you_said": "the problematic phrase",
      "stronger_option_1": "better alternative 1",
      "stronger_option_2": "better alternative 2",
      "stronger_option_3": "better alternative 3",
      "why_it_matters": "brief explanation",
      "accent_note": "pronunciation tip if relevant, else null",
      "delivery_note": "how to deliver it better",
      "confidence_note": "confidence/body language tip if relevant",
      "suggested_drill": "specific practice exercise",
      "recurring": false
    }}
  ],
  "scores": {{
    "overall": 65,
    "accent": 60,
    "confidence": 70,
    "sales": 55,
    "warmth": 75
  }},
  "summary": "2-3 sentence coaching summary — warm, encouraging, specific",
  "top_priority": "The single most important thing to work on",
  "exercises": ["exercise names from the standard drill list that are most relevant"]
}}

COACHING TONE: Warm, encouraging, specific. Never shaming. Like a friendly senior agent helping a teammate improve.
Mark events as "recurring": true if they match known issues from the user's history.
Scores are 0-100 where 50 = average, 70 = good, 90 = excellent."""

    try:
        response = await ask(task="call_coaching", prompt=prompt)
        text_response = response if isinstance(response, str) else str(response)
        start_idx = text_response.find("{")
        end_idx = text_response.rfind("}") + 1
        if start_idx >= 0 and end_idx > start_idx:
            result = json.loads(text_response[start_idx:end_idx])
            return result
        else:
            logger.error("No JSON found in AI coach response")
            return _fallback_coaching(transcript_text, segments)
    except Exception as e:
        logger.error(f"AI coach generation failed: {e}")
        return _fallback_coaching(transcript_text, segments)


def _fallback_coaching(transcript_text: str, segments: list) -> dict:
    """Rule-based fallback when AI is unavailable."""
    events = []
    words = transcript_text.lower().split()

    # Detect fillers
    filler_count = sum(1 for w in words if w.strip(".,!?") in ["um", "uh"])
    if filler_count > 3:
        events.append({
            "timestamp_start_ms": 0,
            "timestamp_end_ms": 0,
            "subtitle_excerpt": f"Found {filler_count} filler words (um/uh)",
            "issue_type": "filler",
            "severity": "moderate" if filler_count > 5 else "minor",
            "what_you_said": f"{filler_count} filler words detected",
            "stronger_option_1": "Pause silently instead of filling with 'um'",
            "stronger_option_2": "Take a breath between thoughts",
            "stronger_option_3": "Slow your pace slightly to reduce fillers",
            "why_it_matters": "Fillers reduce perceived confidence and authority",
            "accent_note": None,
            "delivery_note": "A brief silence is more powerful than a filler",
            "confidence_note": "Fillers often come from rushing — slow down",
            "suggested_drill": "Record yourself for 30 seconds. Count fillers. Repeat until zero.",
            "recurring": False,
        })

    # Check for Australian pronunciation opportunities
    for word, correction in ACCENT_CORRECTIONS.items():
        if word in transcript_text.lower():
            events.append({
                "timestamp_start_ms": 0,
                "timestamp_end_ms": 0,
                "subtitle_excerpt": f"Used '{word}'",
                "issue_type": "accent",
                "severity": "minor",
                "what_you_said": word,
                "stronger_option_1": f"Say '{correction['correct']}' — {correction['note']}",
                "stronger_option_2": f"Practice: '{correction['correct']}' (repeat 3x)",
                "stronger_option_3": f"Natural flow: integrate '{correction['correct']}' into full sentence",
                "why_it_matters": "Sounding natural builds rapport with Australian clients",
                "accent_note": correction["note"],
                "delivery_note": "Let the word flow naturally — don't over-enunciate",
                "confidence_note": None,
                "suggested_drill": f"Say '{correction['correct']}' in a sentence 5 times",
                "recurring": False,
            })

    # Check for booking ask
    booking_words = ["book", "appraisal", "meet", "schedule", "come by", "pop around"]
    has_booking = any(bw in transcript_text.lower() for bw in booking_words)
    if not has_booking:
        events.append({
            "timestamp_start_ms": 0,
            "timestamp_end_ms": 0,
            "subtitle_excerpt": "No booking ask detected in call",
            "issue_type": "close",
            "severity": "critical",
            "what_you_said": "(No clear booking attempt)",
            "stronger_option_1": "Would it be worth me popping around for a quick chat this week?",
            "stronger_option_2": "I've got a spot on Thursday arvo — would that work for a quick appraisal?",
            "stronger_option_3": "Happy to swing by and give you an idea of what the place is worth — no pressure at all.",
            "why_it_matters": "Every call should have a clear, warm ask to meet",
            "accent_note": None,
            "delivery_note": "Keep it casual, low-pressure. Falling tone on the ask.",
            "confidence_note": "Smile while you say it — they can hear it",
            "suggested_drill": "Practice 3 different booking asks. Say each one 5 times with a smile.",
            "recurring": False,
        })

    return {
        "coaching_events": events[:20],
        "scores": {
            "overall": 50,
            "accent": 50,
            "confidence": 50,
            "sales": 40 if not has_booking else 60,
            "warmth": 50,
        },
        "summary": "Call analyzed with rule-based coaching. AI-powered deep analysis available when AI service is connected.",
        "top_priority": "Missing booking ask" if not has_booking else "Reduce filler words",
        "exercises": ["Wide Palate Warm-Up", "Smile While Speaking", "Falling Tone Certainty"],
    }


async def process_call_coaching(
    db_session,
    call_id: int,
    user_id: str = "shahid",
    force: bool = False,
) -> dict:
    """
    Full pipeline: fetch call data, generate coaching, persist results.

    Args:
        db_session: async database session
        call_id: call_log ID
        user_id: operator ID
        force: re-run even if session exists

    Returns:
        Coaching session dict with events and scores
    """
    from sqlalchemy import text

    # Check existing session
    if not force:
        existing = (await db_session.execute(
            text(
                "SELECT id, status, coach_data_json FROM coaching_sessions "
                "WHERE call_id = :cid AND user_id = :uid ORDER BY id DESC LIMIT 1"
            ),
            {"cid": call_id, "uid": user_id},
        )).fetchone()
        if existing and existing[1] == "ready" and existing[2]:
            return json.loads(existing[2])

    # Fetch call + transcript
    call_row = (await db_session.execute(
        text("SELECT id, lead_id, transcript, duration_seconds FROM call_log WHERE id = :cid"),
        {"cid": call_id},
    )).fetchone()

    if not call_row:
        raise ValueError(f"Call {call_id} not found")

    transcript_text = call_row[2] or ""
    lead_id = call_row[1]

    # Fetch segments if available
    segments = []
    seg_rows = (await db_session.execute(
        text(
            "SELECT speaker_id, text, start_ms, end_ms FROM call_segments "
            "WHERE call_id = :cid ORDER BY turn_index"
        ),
        {"cid": call_id},
    )).fetchall()
    for s in seg_rows:
        segments.append({"speaker": s[0], "text": s[1], "start_ms": s[2], "end_ms": s[3]})

    # If no segments, try transcripts table
    if not segments and not transcript_text:
        t_row = (await db_session.execute(
            text("SELECT full_text FROM transcripts WHERE call_id = :cid ORDER BY id DESC LIMIT 1"),
            {"cid": call_id},
        )).fetchone()
        if t_row:
            transcript_text = t_row[0] or ""

    if not transcript_text and not segments:
        return {"error": "No transcript available for this call", "status": "no_transcript"}

    # Fetch existing habits
    habit_rows = (await db_session.execute(
        text(
            "SELECT habit_type, description, example_phrase, corrected_phrase, occurrence_count, mastery_state "
            "FROM speech_habits WHERE user_id = :uid ORDER BY occurrence_count DESC"
        ),
        {"uid": user_id},
    )).fetchall()
    existing_habits = [
        {
            "habit_type": h[0],
            "description": h[1],
            "example_phrase": h[2],
            "corrected_phrase": h[3],
            "occurrence_count": h[4],
            "mastery_state": h[5],
        }
        for h in habit_rows
    ]

    # Create/update session
    now = datetime.now(timezone.utc).isoformat()
    await db_session.execute(
        text(
            "INSERT INTO coaching_sessions (call_id, lead_id, user_id, status, created_at, updated_at) "
            "VALUES (:cid, :lid, :uid, 'processing', :now, :now) "
            "ON CONFLICT (call_id, user_id) DO UPDATE SET status = 'processing', updated_at = :now"
        ),
        {"cid": call_id, "lid": lead_id, "uid": user_id, "now": now},
    )

    try:
        # Generate coaching
        result = await generate_coaching_session(call_id, transcript_text, segments, user_id, existing_habits)

        if "error" in result:
            await db_session.execute(
                text(
                    "UPDATE coaching_sessions SET status = 'failed', updated_at = :now "
                    "WHERE call_id = :cid AND user_id = :uid"
                ),
                {"cid": call_id, "uid": user_id, "now": now},
            )
            await db_session.commit()
            return result

        events = result.get("coaching_events", [])
        scores = result.get("scores", {})

        # Persist coaching events
        for i, evt in enumerate(events):
            await db_session.execute(
                text(
                    "INSERT INTO coaching_events "
                    "(session_id, call_id, timestamp_start_ms, timestamp_end_ms, subtitle_excerpt, "
                    "issue_type, severity, what_you_said, stronger_option_1, stronger_option_2, "
                    "stronger_option_3, why_it_matters, accent_note, delivery_note, confidence_note, "
                    "suggested_drill, recurring, lesson_order, created_at) "
                    "SELECT cs.id, :cid, :ts, :te, :sub, :it, :sev, :wys, :s1, :s2, :s3, :wim, "
                    ":an, :dn, :cn, :sd, :rec, :lo, :now "
                    "FROM coaching_sessions cs WHERE cs.call_id = :cid AND cs.user_id = :uid "
                    "ORDER BY cs.id DESC LIMIT 1"
                ),
                {
                    "cid": call_id,
                    "uid": user_id,
                    "now": now,
                    "ts": evt.get("timestamp_start_ms", 0),
                    "te": evt.get("timestamp_end_ms", 0),
                    "sub": evt.get("subtitle_excerpt", "")[:500],
                    "it": evt.get("issue_type", "wording"),
                    "sev": evt.get("severity", "minor"),
                    "wys": evt.get("what_you_said", "")[:500],
                    "s1": evt.get("stronger_option_1", "")[:500],
                    "s2": evt.get("stronger_option_2", "")[:500],
                    "s3": evt.get("stronger_option_3", "")[:500],
                    "wim": evt.get("why_it_matters", "")[:500],
                    "an": evt.get("accent_note"),
                    "dn": evt.get("delivery_note"),
                    "cn": evt.get("confidence_note"),
                    "sd": evt.get("suggested_drill"),
                    "rec": evt.get("recurring", False),
                    "lo": i,
                },
            )

        # Update habits
        for evt in events:
            if evt.get("issue_type") and evt.get("what_you_said"):
                await _update_habit(db_session, user_id, call_id, evt, now)

        # Update session as ready
        await db_session.execute(
            text(
                "UPDATE coaching_sessions "
                "SET status = 'ready', total_lessons = :tl, overall_score = :os, "
                "accent_score = :as2, confidence_score = :cs, sales_score = :ss, "
                "warmth_score = :ws, coach_data_json = :cdj, updated_at = :now "
                "WHERE call_id = :cid AND user_id = :uid"
            ),
            {
                "cid": call_id,
                "uid": user_id,
                "now": now,
                "tl": len(events),
                "os": scores.get("overall"),
                "as2": scores.get("accent"),
                "cs": scores.get("confidence"),
                "ss": scores.get("sales"),
                "ws": scores.get("warmth"),
                "cdj": json.dumps(result),
            },
        )

        await db_session.commit()

        # Attach exercises and hostile response templates
        result["vocal_exercises"] = [ex for ex in VOCAL_EXERCISES if ex["name"] in result.get("exercises", [])]
        if not result["vocal_exercises"]:
            result["vocal_exercises"] = VOCAL_EXERCISES[:3]

        result["hostile_responses"] = HOSTILE_RESPONSE_TEMPLATES
        result["session_status"] = "ready"

        return result

    except Exception as e:
        logger.error(f"Coach processing failed for call {call_id}: {e}")
        await db_session.execute(
            text(
                "UPDATE coaching_sessions SET status = 'failed', updated_at = :now "
                "WHERE call_id = :cid AND user_id = :uid"
            ),
            {"cid": call_id, "uid": user_id, "now": now},
        )
        await db_session.commit()
        raise


async def _update_habit(db_session, user_id: str, call_id: int, event: dict, now: str):
    """Update or create a speech habit based on a coaching event."""
    from sqlalchemy import text

    habit_type = event["issue_type"]
    description = event.get("why_it_matters", "")[:200]
    example = event.get("what_you_said", "")[:200]
    corrected = event.get("stronger_option_1", "")[:200]

    existing = (await db_session.execute(
        text(
            "SELECT id, occurrence_count FROM speech_habits "
            "WHERE user_id = :uid AND habit_type = :ht AND example_phrase = :ep LIMIT 1"
        ),
        {"uid": user_id, "ht": habit_type, "ep": example},
    )).fetchone()

    if existing:
        await db_session.execute(
            text(
                "UPDATE speech_habits "
                "SET occurrence_count = occurrence_count + 1, last_seen_call_id = :cid, updated_at = :now, "
                "mastery_state = CASE WHEN occurrence_count > 5 THEN 'needs_work' ELSE mastery_state END "
                "WHERE id = :hid"
            ),
            {"hid": existing[0], "cid": call_id, "now": now},
        )
    else:
        await db_session.execute(
            text(
                "INSERT INTO speech_habits "
                "(user_id, habit_type, description, example_phrase, corrected_phrase, "
                "severity, occurrence_count, first_seen_call_id, last_seen_call_id, mastery_state, created_at, updated_at) "
                "VALUES (:uid, :ht, :desc, :ep, :cp, :sev, 1, :cid, :cid, 'needs_work', :now, :now)"
            ),
            {
                "uid": user_id,
                "ht": habit_type,
                "desc": description,
                "ep": example,
                "cp": corrected,
                "sev": event.get("severity", "minor"),
                "cid": call_id,
                "now": now,
            },
        )


async def get_speech_profile(db_session, user_id: str = "shahid") -> dict:
    """Get the user's speech habit profile with improvement tracking."""
    from sqlalchemy import text

    habits = (await db_session.execute(
        text(
            "SELECT habit_type, description, example_phrase, corrected_phrase, severity, "
            "occurrence_count, first_seen_call_id, last_seen_call_id, mastery_state, last_improved_at "
            "FROM speech_habits WHERE user_id = :uid ORDER BY occurrence_count DESC"
        ),
        {"uid": user_id},
    )).fetchall()

    sessions = (await db_session.execute(
        text(
            "SELECT overall_score, accent_score, confidence_score, sales_score, warmth_score, created_at "
            "FROM coaching_sessions WHERE user_id = :uid AND status = 'ready' ORDER BY created_at DESC LIMIT 20"
        ),
        {"uid": user_id},
    )).fetchall()

    return {
        "habits": [
            {
                "habit_type": h[0],
                "description": h[1],
                "example_phrase": h[2],
                "corrected_phrase": h[3],
                "severity": h[4],
                "occurrence_count": h[5],
                "first_seen_call_id": h[6],
                "last_seen_call_id": h[7],
                "mastery_state": h[8],
                "last_improved_at": h[9],
                "status_label": (
                    "Still recurring" if h[5] > 3 and h[8] == "needs_work"
                    else "Improving" if h[8] == "improving"
                    else "Mastered" if h[8] == "mastered"
                    else "Needs work"
                ),
            }
            for h in habits
        ],
        "score_trend": [
            {"overall": s[0], "accent": s[1], "confidence": s[2], "sales": s[3], "warmth": s[4], "date": s[5]}
            for s in sessions
        ],
        "total_sessions": len(sessions),
        "total_habits_tracked": len(habits),
        "mastered_count": sum(1 for h in habits if h[8] == "mastered"),
        "needs_work_count": sum(1 for h in habits if h[8] == "needs_work"),
        "vocal_exercises": VOCAL_EXERCISES,
    }


async def get_preferences(db_session, user_id: str = "shahid") -> dict:
    """Get coach preferences for user."""
    from sqlalchemy import text

    row = (await db_session.execute(
        text("SELECT * FROM coach_preferences WHERE user_id = :uid"),
        {"uid": user_id},
    )).fetchone()

    if row:
        return dict(row._mapping)

    # Return defaults
    return {
        "user_id": user_id,
        "display_name": "Shahid",
        "coach_name": "Coach",
        "coach_voice": "female",
        "lesson_repeat_count": 2,
        "playback_speed": 1.0,
        "subtitle_density": "normal",
        "accent_focus_level": "high",
        "sales_focus_level": "high",
        "auto_open_first_lesson": True,
        "prioritize": "balanced",
        "keyboard_hints": True,
    }


async def update_preferences(db_session, user_id: str, prefs: dict) -> dict:
    """Update coach preferences."""
    from sqlalchemy import text

    now = datetime.now(timezone.utc).isoformat()
    fields = [
        "display_name", "coach_name", "coach_voice", "lesson_repeat_count",
        "playback_speed", "subtitle_density", "accent_focus_level", "sales_focus_level",
        "auto_open_first_lesson", "prioritize", "keyboard_hints",
    ]

    updates = {k: v for k, v in prefs.items() if k in fields}
    if not updates:
        return await get_preferences(db_session, user_id)

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)

    await db_session.execute(
        text(
            f"INSERT INTO coach_preferences (user_id, created_at, updated_at, {', '.join(updates.keys())}) "
            f"VALUES (:uid, :now, :now, {', '.join(f':{k}' for k in updates)}) "
            f"ON CONFLICT (user_id) DO UPDATE SET {set_clause}, updated_at = :now"
        ),
        {"uid": user_id, "now": now, **updates},
    )
    await db_session.commit()

    return await get_preferences(db_session, user_id)
