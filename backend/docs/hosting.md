# Hosting

## Current State

Cloud deployment now has two distinct paths:

- Render native, no Docker: preferred for this repo now
- legacy container hosting docs below: still useful as fallback reference

The active production target is the Render-native setup documented in [`docs/production-deployment.md`](/D:/woonona-lead-machine/docs/production-deployment.md) and encoded in [`render.yaml`](/D:/woonona-lead-machine/render.yaml).

Legacy container artifacts still in the repo:

- `docker-compose.cloud.yml` - canonical cloud stack
- `backend/Dockerfile.cloud` - backend image
- `frontend/Dockerfile` - static frontend image served by Caddy
- `frontend/Caddyfile` - HTTPS + beta-auth + reverse proxy
- `deploy/oci/bootstrap-ubuntu.sh` - base VM bootstrap
- `deploy/oci/bootstrap-stack.sh` - stack bring-up and migration helper

## Recommendation: OCI Always Free First

If the goal is "public soon, free, and close to the existing code", the best path is an
Oracle Cloud Infrastructure Always Free ARM VM running the existing Docker Compose stack.

This app is not just a request/response API. It currently depends on:

- long-lived background loops started in `backend/main.py`
- Redis
- Postgres extensions (`postgis`, `h3`, `pgvector`)
- native Linux packages for PDF, OCR, and image/report work
- local file mirroring for evidence and generated artifacts

That makes a single container host the cleanest first deployment shape.

### Why OCI wins for this repo

- The repo already ships an OCI-oriented deployment path.
- Oracle's Always Free tier includes ARM compute, block volume, object storage, load balancer, and related services.
- Ampere A1 Always Free capacity is large enough to run this stack as a private beta on one box.
- No serverless rewrites are required before launch.

Official references:

- Oracle Cloud Free Tier: <https://www.oracle.com/cloud/free/>
- Oracle Ampere A1 pricing: <https://www.oracle.com/cloud/compute/arm/pricing/>

## Day-1 Deployment Shape

Run these services on one OCI ARM VM:

- `frontend`
- `backend`
- `postgres`
- `redis`

Keep `n8n` off until you actually need remote automation editing. It is optional for the app to run.

## What To Do

1. Create an OCI account and choose a region with Ampere A1 availability.
2. Provision an Ubuntu ARM VM.
3. Point DNS for `APP_DOMAIN` to the VM public IP.
4. Clone the repo onto the VM.
5. Run `bash deploy/oci/quick-deploy.sh`.
6. Answer the prompts for domain and beta access.
7. Mirror private evidence files from the Windows machine if cloud access to the archive matters.

`deploy/oci/quick-deploy.sh` now handles host bootstrap, env generation, service startup,
and SQLite -> Postgres migration when `leads.db` is present.

## What Not To Do Yet

- Do not move the current backend straight to edge/serverless runtimes.
- Do not keep SQLite as the production database once multiple operators are involved.
- Do not rely on the embedded frontend API key as a real auth model.
- Do not expose raw file-path storage as the long-term artifact strategy.

## Scalable Path After Launch

### Phase 1: Single Operator Private Beta

- One OCI VM
- Postgres + Redis on-box
- Caddy in front
- Private beta auth

### Phase 2: Multi-Operator Product

- Move evidence and generated reports to object storage
- Split background work out of web request handling
- Add proper user auth and per-user audit trails
- Keep Postgres as the system of record

### Phase 3: SaaS Shape

- Edge frontend on Cloudflare Pages/Workers
- Containerized API separated from workers
- Managed Postgres
- Object storage for files
- Queue-driven ingestion and enrichment
- Tenant-aware schema and auth

See `docs/deploy-now.md` for the full operator-first and SaaS roadmap.

## DATABASE_URL Logic

- `USE_POSTGRES=false`: SQLite at repo root `leads.db`
- `USE_POSTGRES=true`: backend reads `DATABASE_URL`

In the cloud compose stack, `DATABASE_URL` is injected automatically for the internal
Postgres container.

## Ports

- Backend: `8001` (configurable via `BACKEND_PORT`)
- Frontend dev: `5174`
- Frontend prod: `80` and `443` through Caddy
