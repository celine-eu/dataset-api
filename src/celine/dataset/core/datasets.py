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
