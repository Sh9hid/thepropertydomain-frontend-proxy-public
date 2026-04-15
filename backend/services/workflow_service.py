"""
Workflow / cadence scheduling service — thin re-export layer.
Route handlers import from here rather than directly from core.logic /
services.automations, establishing a clean service boundary.
"""
from core.logic import (  # noqa: F401
    _next_business_slot,
    _append_stage_note,
    _append_activity,
    _build_activity_entry,
    _recent_touch_count,
    _append_activity_and_commit,
)
from services.automations import (  # noqa: F401
    _supersede_auto_tasks,
    _schedule_callback_cadence,
    _schedule_nurture_cadence,
    _schedule_enrichment_task,
    _schedule_booked_followthrough,
)

__all__ = [
    "_next_business_slot",
    "_append_stage_note",
    "_append_activity",
    "_build_activity_entry",
    "_recent_touch_count",
    "_append_activity_and_commit",
    "_supersede_auto_tasks",
    "_schedule_callback_cadence",
    "_schedule_nurture_cadence",
    "_schedule_enrichment_task",
    "_schedule_booked_followthrough",
]
