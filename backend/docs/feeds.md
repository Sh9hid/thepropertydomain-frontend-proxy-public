# Feeds

## REAXML Agency Feeds

Polled every 15 minutes by `_reaxml_poll_loop` in `backend/services/reaxml_ingestor.py`.

**Status as of 2026-03-22**:

| Count | Status |
|---|---|
| 3 | Live (confirmed working URLs) |
| 13 | Stubbed (no public feed, 403, or custom platform) |

The 3 live agencies are in `backend/core/config.py:AGENCY_FEEDS_2765` — Stone Real Estate Hawkesbury plus 2 others.

To add a feed: paste the URL in `AGENCY_FEEDS_2765` or `AGENCY_FEEDS_2517` and restart the backend. The ingestor polls automatically.

## Domain Withdrawn

- **Service**: `backend/services/domain_withdrawn.py`
- **Schedule**: Daily 7:00 AM AEST
- **Auth**: Requires `DOMAIN_CLIENT_ID` and `DOMAIN_CLIENT_SECRET` in `.env`
- **Coverage**: All suburbs in `ALL_TARGET_SUBURBS` (2765 + 2517/18 corridors)
- **Output**: Upserts withdrawn listings as leads with `trigger_type = domain_withdrawn`

## NSW Planning Portal (DA Feed)

- **Service**: `backend/services/da_feed_ingestor.py`
- **Schedule**: Daily 7:30 AM AEST
- **Auth**: None — free public API
- **Coverage**: Postcodes 2765, 2517, 2518, 2756, 2775
- **Output**: Upserts development applications as leads with `trigger_type = Development Application`

## NSW Probate Gazette

- **Service**: `backend/scraper.py` (`scrape_nsw_probate_market`)
- **Schedule**: Daily 6:00 AM AEST
- **Auth**: None — scrapes public gazette
- **Output**: Upserts probate listings as leads with `trigger_type = probate`

## Domain Enrichment

- **Service**: `backend/services/domain_enrichment.py`
- **Schedule**: Hourly during business hours (7am–10pm AEST)
- **Auth**: Requires `DOMAIN_CLIENT_ID` and `DOMAIN_CLIENT_SECRET`
- **Purpose**: Enriches existing leads with Domain API data (photos, est_value, bedrooms, etc.)

## Sitemap / Marketing List

- **Service**: `backend/services/sitemap_ingestor.py`
- **Schedule**: Periodic
- **Purpose**: Validates and ingests leads from sitemaps
