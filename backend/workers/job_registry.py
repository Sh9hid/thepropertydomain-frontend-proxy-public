from __future__ import annotations

VALID_RUNTIME_ROLES = ("web", "worker", "scheduler")

WORKER_RUNTIME_TASKS = (
    "_background_sender_loop",
    "_control_runtime_loop",
    "_orchestration_loop",
)

SCHEDULER_RUNTIME_TASKS = (
    "_system_health_pulse",
    "_reaxml_poll_loop",
    "_sitemap_validation_loop",
    "_daily_delta_loop",
    "_domain_enrichment_loop",
    "_geocoding_loop",
    "_domain_ingest_loop",
    "_da_feed_loop",
    "_auto_ticket_loop",
    "_research_scheduler_loop",
    "_hermes_department_scheduler_loop",
    "_self_improvement_loop",
    "_door_knock_excel_sync_loop",
    "_nyla_pipeline_loop",
    "_rex_listing_loop",
    "_reactive_scoring_loop",
    "_outreach_sender_loop",
)


def get_runtime_task_names(runtime_role: str) -> tuple[str, ...]:
    role = (runtime_role or "web").strip().lower()
    if role == "web":
        return ()
    if role == "worker":
        return WORKER_RUNTIME_TASKS
    if role == "scheduler":
        return SCHEDULER_RUNTIME_TASKS
    raise ValueError(f"Unsupported runtime role: {runtime_role!r}")
