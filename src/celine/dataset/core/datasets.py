# dataset/core/datasets.py
from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, or_, select
from fastapi import HTTPException

from celine.dataset.db.models.dataset_entry import DatasetEntry


async def load_dataset_entry(*, db: AsyncSession, dataset_id: str) -> DatasetEntry:
    """Load an entry by id, whatever its exposure.

    For the query path, which enforces access through governance and OPA rather
    than through catalogue visibility. Anything that *shows* a dataset to a
    caller wants `load_catalogue_entry` instead.
    """
    res = await db.execute(
        select(DatasetEntry).where(DatasetEntry.dataset_id == dataset_id)
    )
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return entry


def catalogue_visible(stmt: Select) -> Select:
    """Restrict a `DatasetEntry` query to what the catalogue may show.

    One rule for every catalogue surface — the JSON-LD documents, the schema and
    vocabulary sub-resources, and the HTML pages — because they all answer the
    same unauthenticated caller and a surface that disagreed would be the leak.

    Two governance flags, and only `expose` is one of them. `expose` is the
    catalogue gate; `dataspace_expose` is consent to *offer the dataset into the
    dataspace*, checked by the EDR path in the query executor, and it grants
    nothing here — a dataspace offer is made to a contracted consumer, not to
    whoever opens the catalogue in a browser. The export refuses the one
    incoherent pairing (offered to the dataspace, absent from the catalogue), so
    for exported data this is the same set either way; the difference only shows
    for rows written straight through the admin API.

    `secret` is dropped on top of that: `expose` says the dataset is listed,
    `access_level` says how much of it anyone may see, and `secret` means not
    even its metadata. A NULL `access_level` is not secret — SQL would drop the
    row on a bare `!=` comparison.
    """
    return stmt.where(
        DatasetEntry.expose.is_(True),
        or_(
            DatasetEntry.access_level.is_(None),
            DatasetEntry.access_level != "secret",
        ),
    )


async def load_catalogue_entry(*, db: AsyncSession, dataset_id: str) -> DatasetEntry:
    """Load an entry the catalogue is allowed to show, or 404.

    404 rather than 403: a caller who may not see the dataset may not learn it
    exists either.
    """
    stmt = catalogue_visible(
        select(DatasetEntry).where(DatasetEntry.dataset_id == dataset_id)
    )
    res = await db.execute(stmt)
    entry = res.scalars().first()
    if not entry:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return entry


async def list_catalogue_entries(*, db: AsyncSession) -> list[DatasetEntry]:
    """Every entry the catalogue may show, ordered by id."""
    stmt = catalogue_visible(select(DatasetEntry)).order_by(DatasetEntry.dataset_id)
    res = await db.execute(stmt)
    return list(res.scalars().all())


#: The only backend this service can run SQL against. Entries of any other type
#: (s3, fs, …) are catalogue-only: listed, never queryable.
QUERYABLE_BACKEND = "postgres"


def derive_physical_table(dataset_id: str) -> str:
    """The physical `schema.table` a dataset id names by convention.

    "datasets.ds_dev_gold.foo"  -> "ds_dev_gold.foo"
    "singer.tap-test.foo"       -> "tap-test.foo"
    "schema.table"              -> "schema.table"  (already 2-part, kept as-is)

    The one rule for it: `export governance` writes this as `backend_config.table`,
    and the query path falls back to it when an entry states no table.
    """
    parts = dataset_id.split(".")
    if len(parts) >= 3:
        return ".".join(parts[1:])
    return dataset_id


def physical_table(
    dataset_id: str,
    backend_type: str | None,
    backend_config: dict | None,
) -> str | None:
    """The table a dataset is queried through, or None when it has none.

    `backend_config.table` when stated; for a postgres entry that states none,
    the table its id names (`derive_physical_table`). Every consumer — the query
    path, the import's existence check and the stale-entry cleanup — resolves it
    here, so an entry is kept exactly when the table it would be queried through
    exists, and a query never reaches SQL under a name nobody resolved.
    """
    if backend_type != QUERYABLE_BACKEND:
        return None
    table = (backend_config or {}).get("table")
    return table or derive_physical_table(dataset_id)
