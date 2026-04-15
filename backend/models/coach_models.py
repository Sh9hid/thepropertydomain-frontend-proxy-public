"""SQLModel tables for the AI Coach feature."""
from typing import Optional
from sqlmodel import SQLModel, Field


class CoachingSession(SQLModel, table=True):
    __tablename__ = "coaching_sessions"

    id: Optional[int] = Field(default=None, primary_key=True)
    call_id: int
    lead_id: Optional[int] = None
    user_id: str = "shahid"
    status: str = "pending"  # pending|processing|ready|failed
    total_lessons: int = 0
    completed_lessons: int = 0
    skipped_lessons: int = 0
    overall_score: Optional[float] = None
    accent_score: Optional[float] = None
    confidence_score: Optional[float] = None
    sales_score: Optional[float] = None
    warmth_score: Optional[float] = None
    coach_data_json: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class CoachingEvent(SQLModel, table=True):
    __tablename__ = "coaching_events"

    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: int
    call_id: int
    timestamp_start_ms: int = 0
    timestamp_end_ms: int = 0
    subtitle_excerpt: str = ""
    issue_type: str = ""  # accent|wording|confidence|objection|warmth|opener|close|recovery|pacing|filler
    severity: str = "minor"  # minor|moderate|critical
    what_you_said: str = ""
    stronger_option_1: str = ""
    stronger_option_2: str = ""
    stronger_option_3: str = ""
    why_it_matters: str = ""
    accent_note: Optional[str] = None
    delivery_note: Optional[str] = None
    confidence_note: Optional[str] = None
    suggested_drill: Optional[str] = None
    recurring: bool = False
    lesson_order: int = 0
    created_at: Optional[str] = None


class SpeechHabit(SQLModel, table=True):
    __tablename__ = "speech_habits"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = "shahid"
    habit_type: str = ""  # pronunciation|filler|inflection|pacing|confidence_drop|weak_opener|missed_close|robotic_phrase|poor_recovery
    description: str = ""
    example_phrase: str = ""
    corrected_phrase: str = ""
    severity: str = "minor"
    occurrence_count: int = 1
    first_seen_call_id: Optional[int] = None
    last_seen_call_id: Optional[int] = None
    last_improved_at: Optional[str] = None
    mastery_state: str = "needs_work"  # needs_work|improving|mastered
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class LessonProgress(SQLModel, table=True):
    __tablename__ = "lesson_progress"

    id: Optional[int] = Field(default=None, primary_key=True)
    session_id: int
    event_id: int
    user_id: str = "shahid"
    status: str = "pending"  # pending|viewed|completed|skipped
    repeat_count: int = 0
    preferred_option: Optional[int] = None  # 1, 2, or 3
    notes: Optional[str] = None
    completed_at: Optional[str] = None


class CoachPreferences(SQLModel, table=True):
    __tablename__ = "coach_preferences"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(unique=True, default="shahid")
    display_name: str = "Shahid"
    coach_name: str = "Coach"
    coach_voice: str = "female"  # male|female
    lesson_repeat_count: int = 2
    playback_speed: float = 1.0
    subtitle_density: str = "normal"  # minimal|normal|detailed
    accent_focus_level: str = "high"  # low|medium|high
    sales_focus_level: str = "high"
    auto_open_first_lesson: bool = True
    prioritize: str = "balanced"  # pronunciation|sales|balanced
    keyboard_hints: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
