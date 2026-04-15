# Leads

## Lead Lifecycle

Leads flow through these statuses:

```
captured → active → contacted → qualified → outreach_ready → booked → converted
                                                                    ↘ dropped
```

## Status Transitions

Valid transitions are defined in `backend/models/schemas.py:LEAD_STATUS_TRANSITIONS`.

## Route Queue

Each lead is assigned to one of three pipelines:

| Value | Pipeline | Trigger |
|---|---|---|
| `RE` | L+S Real Estate | Default |
| `MORTGAGE` | Ownit1st Refinance | Settlement 2.5–3.5 years ago |
| `DEVELOPMENT` | Subdivision / DA | H3 hex has nearby DA activity |

**Note**: The routing logic runs via `delta_engine` which requires the PostgreSQL `intelligence` schema. On SQLite (local dev), all leads default to `RE`.

## Signal Status

The `signal_status` field is computed at hydration time:

| Value | Meaning |
|---|---|
| `LIVE` | Has a phone number or marketing list origin |
| `WITHDRAWN` | Trigger is `domain_withdrawn` or archetype is withdrawn |
| `DELTA` | Trigger is `delta_engine` or `domain_withdrawn` |
| `OFF-MARKET` | No phone, no withdrawn signal |
| `SOLD` | Status is `dropped` |
| `UNDER-OFFER` | Under offer status |

## Lead Sources

- **NSW Probate Gazette** — scraped daily at 6 AM
- **Domain Withdrawn** — withdrawn listings from Domain API, daily at 7 AM
- **NSW Planning Portal (DA feed)** — development applications, daily at 7:30 AM
- **REAXML feeds** — agency listings, polled every 15 min
- **Manual entry** — `POST /api/leads/manual`
- **Marketing list** — uploaded via ingest endpoint

## Scoring

See `scoring.md` for formula details.

## Queue Buckets

| Bucket | Meaning |
|---|---|
| `active` | Needs immediate action |
| `callback_due` | Callback scheduled |
| `booked` | Appraisal booked |
| `nurture` | Long-term nurture |
| `enrichment` | Needs data enrichment |
