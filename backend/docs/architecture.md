# Architecture

## Overview

The woonona-lead-machine is a full-stack property intelligence and outreach tool for **Laing+Simmons Oakville | Windsor** operated by Shahid.

## Components

- **Backend**: FastAPI (Python) — runs on port 8001
- **Frontend**: React/TypeScript (Vite) — runs on port 5174
- **Database**: SQLite (local dev) or PostgreSQL (production via `USE_POSTGRES=true`)

## Data Flow

```
Lead Sources → Ingestors → leads table → Intelligence Engine → Frontend
                               ↑
                        Domain Enrichment
                        REAXML Feed
                        Probate Scraper
                        DA Feed (NSW Planning Portal)
                        Domain Withdrawn
```

## Background Loops

| Loop | Schedule | Purpose |
|---|---|---|
| `_probate_scraper_loop` | Daily 6:00 AM AEST | NSW Gazette probate leads |
| `_domain_withdrawn_loop` | Daily 7:00 AM AEST | Domain withdrawn listings |
| `_da_feed_loop` | Daily 7:30 AM AEST | NSW Planning Portal DAs |
| `_domain_enrichment_loop` | Hourly (7am–10pm) | Domain API enrichment |
| `_reaxml_poll_loop` | Every 15 minutes | Agency REAXML feeds |
| `_daily_delta_loop` | Daily | Delta engine scoring |
| `_sitemap_validation_loop` | Periodic | Sitemap validation |
| `_background_sender_loop` | Continuous | Outreach queue sender |
| `_system_health_pulse` | Continuous | Health monitoring |

## Database Paths

- **SQLite**: `leads.db` at project root — used when `USE_POSTGRES=false` (default)
- **PostgreSQL**: Set `USE_POSTGRES=true` and `DATABASE_URL` in `.env` — used for production
- **Intelligence schema**: PostgreSQL only (`intelligence` schema with pgvector)
- **Redis**: Optional — used for caching, falls back gracefully

## Key Config Variables

- `USE_POSTGRES` — `true`/`false` (default: `false`)
- `DATABASE_URL` — PostgreSQL DSN (only read when `USE_POSTGRES=true`)
- `DOMAIN_CLIENT_ID` / `DOMAIN_CLIENT_SECRET` — Domain API credentials
- `API_KEY` — Backend API key (`HILLS_SECURE_2026_CORE` default)
- `BACKEND_PORT` — Backend port (default: `8001`)
