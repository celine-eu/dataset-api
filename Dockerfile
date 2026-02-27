# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ---- builder ---------------------------------------------------------------
FROM base AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml README.md uv.lock ./

RUN uv sync --frozen --no-install-project --no-dev

COPY src ./src
COPY policies ./policies
COPY alembic ./alembic
COPY alembic.ini ./

RUN uv pip install --no-deps .

# ---- runtime ---------------------------------------------------------------
FROM base AS runtime

RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid app --no-create-home app

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv    /app/.venv
COPY --from=builder --chown=app:app /app/src       /app/src
COPY --from=builder --chown=app:app /app/policies  /app/policies
COPY --from=builder --chown=app:app /app/alembic   /app/alembic
COPY --from=builder --chown=app:app /app/alembic.ini /app/alembic.ini

ENV PATH="/app/.venv/bin:${PATH}" \
    VIRTUAL_ENV="/app/.venv"

USER app

EXPOSE 8001

CMD ["uvicorn", "celine.dataset.main:create_app", "--factory", \
    "--host", "0.0.0.0", "--port", "8001"]