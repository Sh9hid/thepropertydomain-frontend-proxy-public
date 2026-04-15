# ── Stage 1: builder ─────────────────────────────────────────────────────────
# Has compiler toolchain to build C-extension wheels.
# Nothing from this stage reaches the final image except /install.
FROM python:3.10-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Compile-time system deps only (headers + compilers).
# libgdal-dev is intentionally excluded — no Python code imports GDAL.
RUN apt-get update && apt-get install -y --no-install-recommends --fix-missing \
    build-essential \
    g++ \
    libffi-dev \
    libpq-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libjpeg62-turbo-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-prod.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements-prod.txt


# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Lean final image — no compilers, no -dev headers, no libgdal.
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/install/lib/python3.10/site-packages \
    PATH=/install/bin:$PATH \
    APP_ENV=production \
    BACKEND_PORT=8001

WORKDIR /app

# Runtime-only system libs (no -dev variants, no GDAL).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libffi8 \
    libpq5 \
    libxml2 \
    libxslt1.1 \
    zlib1g \
    libjpeg62-turbo \
    libcairo2 \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    shared-mime-info \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder.
COPY --from=builder /install /install

# Copy application source (tightened by .dockerignore).
COPY . .

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app /install

USER appuser

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s --retries=5 CMD curl -fsS http://127.0.0.1:8001/livez || exit 1

CMD ["python", "-m", "uvicorn", "runtime.app:app", "--host", "0.0.0.0", "--port", "8001"]
