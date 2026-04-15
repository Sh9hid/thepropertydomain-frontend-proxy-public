"""Create coaching tables for AI Coach feature."""
import asyncio
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

COACH_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS coaching_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id INTEGER NOT NULL,
    lead_id INTEGER,
    user_id TEXT NOT NULL DEFAULT 'shahid',
    status TEXT NOT NULL DEFAULT 'pending',
    total_lessons INTEGER DEFAULT 0,
    completed_lessons INTEGER DEFAULT 0,
    skipped_lessons INTEGER DEFAULT 0,
    overall_score REAL,
    accent_score REAL,
    confidence_score REAL,
    sales_score REAL,
    warmth_score REAL,
    coach_data_json TEXT,
    created_at TEXT,
    updated_at TEXT,
    UNIQUE(call_id, user_id)
);

CREATE TABLE IF NOT EXISTS coaching_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    call_id INTEGER NOT NULL,
    timestamp_start_ms INTEGER DEFAULT 0,
    timestamp_end_ms INTEGER DEFAULT 0,
    subtitle_excerpt TEXT DEFAULT '',
    issue_type TEXT DEFAULT '',
    severity TEXT DEFAULT 'minor',
    what_you_said TEXT DEFAULT '',
    stronger_option_1 TEXT DEFAULT '',
    stronger_option_2 TEXT DEFAULT '',
    stronger_option_3 TEXT DEFAULT '',
    why_it_matters TEXT DEFAULT '',
    accent_note TEXT,
    delivery_note TEXT,
    confidence_note TEXT,
    suggested_drill TEXT,
    recurring BOOLEAN DEFAULT 0,
    lesson_order INTEGER DEFAULT 0,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS speech_habits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL DEFAULT 'shahid',
    habit_type TEXT DEFAULT '',
    description TEXT DEFAULT '',
    example_phrase TEXT DEFAULT '',
    corrected_phrase TEXT DEFAULT '',
    severity TEXT DEFAULT 'minor',
    occurrence_count INTEGER DEFAULT 1,
    first_seen_call_id INTEGER,
    last_seen_call_id INTEGER,
    last_improved_at TEXT,
    mastery_state TEXT DEFAULT 'needs_work',
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS lesson_progress (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    event_id INTEGER NOT NULL,
    user_id TEXT NOT NULL DEFAULT 'shahid',
    status TEXT DEFAULT 'pending',
    repeat_count INTEGER DEFAULT 0,
    preferred_option INTEGER,
    notes TEXT,
    completed_at TEXT,
    UNIQUE(event_id, user_id)
);

CREATE TABLE IF NOT EXISTS coach_preferences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT NOT NULL UNIQUE DEFAULT 'shahid',
    display_name TEXT DEFAULT 'Shahid',
    coach_name TEXT DEFAULT 'Coach',
    coach_voice TEXT DEFAULT 'female',
    lesson_repeat_count INTEGER DEFAULT 2,
    playback_speed REAL DEFAULT 1.0,
    subtitle_density TEXT DEFAULT 'normal',
    accent_focus_level TEXT DEFAULT 'high',
    sales_focus_level TEXT DEFAULT 'high',
    auto_open_first_lesson BOOLEAN DEFAULT 1,
    prioritize TEXT DEFAULT 'balanced',
    keyboard_hints BOOLEAN DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_coaching_sessions_call ON coaching_sessions(call_id);
CREATE INDEX IF NOT EXISTS idx_coaching_events_session ON coaching_events(session_id);
CREATE INDEX IF NOT EXISTS idx_coaching_events_call ON coaching_events(call_id);
CREATE INDEX IF NOT EXISTS idx_speech_habits_user ON speech_habits(user_id);
CREATE INDEX IF NOT EXISTS idx_lesson_progress_event ON lesson_progress(event_id)
"""


async def create_coach_tables():
    """Create all coaching tables."""
    from core.database import get_session

    async for db in get_session():
        for statement in COACH_TABLES_SQL.strip().split(";"):
            statement = statement.strip()
            if statement:
                try:
                    await db.execute(text(statement))
                except Exception as e:
                    logger.warning(f"Table creation warning: {e}")
        await db.commit()
        logger.info("Coach tables created successfully")
        break


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(create_coach_tables())
