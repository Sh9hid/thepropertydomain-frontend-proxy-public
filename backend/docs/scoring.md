# Scoring

## Overview

Three scores are computed per lead:

| Score | Range | Purpose |
|---|---|---|
| `heat_score` | 0–100 | Overall urgency / lead temperature |
| `call_today_score` | 0–100 | How likely a call today will convert |
| `evidence_score` | 0–100 | Quality and quantity of data backing the lead |
| `readiness_score` | 0–100 | How ready the lead is for an appraisal pitch |

## heat_score Formula

Base components (from `backend/services/scoring.py`):

- Trigger bonus: `_trigger_bonus(trigger_type)` — probate/withdrawn/DA signals add weight
- Status penalty: `_status_penalty(status)` — dropped/booked leads reduce heat
- Recency: leads under 14 days get bonus points
- Phone presence: +10 if `contact_phones` is non-empty
- Mortgage cliff: +15 if `settlement_date` is 2.5–3.5 years ago

## call_today_score Formula

- Base: `heat_score`
- +20 if `call_today_flag` is set
- +10 if `preferred_channel` is `phone`
- -10 if last touch was within 48h
- Clamped to 0–100

## evidence_score Formula

- +15 per data source in `source_evidence`
- +10 if `domain_listing_id` present
- +10 if `property_images` is non-empty
- +5 if `est_value` present
- +5 if `bedrooms` / `bathrooms` present
- Clamped to 0–100

## readiness_score Formula

- Base: evidence_score × 0.4 + heat_score × 0.6
- Clamped to 0–100

## Score Thresholds (UI)

| Score | Color |
|---|---|
| ≥ 75 | Green (#30d158) |
| 50–74 | Amber (#ff9f0a) |
| < 50 | Red (#ff453a) |

## Routing Weights

Leads with `call_today_score >= 80` are candidates for auto-mission creation (planned, not yet wired).
