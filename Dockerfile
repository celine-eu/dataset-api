# ------------------------------------------------------------
# 1) BUILDER — Install deps with uv in a clean environment
# ------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS builder

WORKDIR /app

# Copy dependency definitions
COPY pyproject.toml poetry.lock* uv.lock* /app/

# Install dependencies into a local prefix
RUN uv pip install --system --no-cache .

# Copy application source
COPY celine /app/src/celine
COPY tests /app/tests

# ------------------------------------------------------------
# 2) RUNTIME — Minimal production image
# ------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UVICORN_WORKERS=2 \
    UVICORN_HOST=0.0.0.0 \
    UVICORN_PORT=8000 \
    LOG_LEVEL=info

WORKDIR /app

# Install system deps (needed for asyncpg)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed site-packages from builder
COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application source
COPY src/celine /app/celine

# Create non-root user
RUN useradd -u 10001 -m appuser
USER appuser

# Expose API port
EXPOSE 8000

# Optional: Docker healthcheck
HEALTHCHECK --interval=20s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default entrypoint
CMD ["uvicorn ", "dataset.main:create_app", "--host ${UVICORN_HOST}", "--port ${UVICORN_PORT}", "--factory", "--workers ${UVICORN_WORKERS}", "--log-level ${LOG_LEVEL}" ]