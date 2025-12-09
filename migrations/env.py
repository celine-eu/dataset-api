# migrations/env.py
from __future__ import annotations

import os
import asyncio
from logging.config import fileConfig
from alembic import context
from sqlalchemy import pool, text
from sqlalchemy.ext.asyncio import create_async_engine

from dotenv import load_dotenv

# Load .env before settings
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(env_path)

from dataset.core.config import settings
from dataset.api.catalogue.models import Base


config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


# ────────────────────────────────────────────
# Restrict Alembic to *your* schema only
# ────────────────────────────────────────────
def include_object(object, name, type_, reflected, compare_to):
    schema = settings.catalogue_schema

    if type_ == "table":
        model_schema = object.schema or schema
        db_schema = compare_to.schema if compare_to is not None else schema
        return model_schema == db_schema == schema

    return True


def include_name(name, type_, parent_names):
    schema = settings.catalogue_schema
    if type_ == "schema":
        # Tell Alembic to include ONLY your schema and not 'public'
        return name == schema
    return True


# ────────────────────────────────────────────
# OFFLINE migrations
# ────────────────────────────────────────────


def run_migrations_offline() -> None:
    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")

    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object,
        include_name=include_name,
        compare_server_default=True,
        compare_type=True,
        version_table_schema=settings.catalogue_schema,
        include_schemas=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


# ────────────────────────────────────────────
# Helper: run migrations inside a sync conn
# ────────────────────────────────────────────
def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=include_object,
        include_name=include_name,
        compare_server_default=True,
        compare_type=True,
        include_schemas=True,
        version_table_schema=settings.catalogue_schema,
    )
    context.run_migrations()


# ────────────────────────────────────────────
# ONLINE migrations (async)
# ────────────────────────────────────────────
async def run_migrations_online() -> None:
    url = settings.database_url.replace("postgresql+psycopg", "postgresql+asyncpg")

    engine = create_async_engine(
        url,
        poolclass=pool.NullPool,
        future=True,
    )

    async with engine.begin() as conn:
        await conn.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{settings.catalogue_schema}"')
        )
        await conn.run_sync(do_run_migrations)

    await engine.dispose()


# ────────────────────────────────────────────
# Entrypoint
# ────────────────────────────────────────────
if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
