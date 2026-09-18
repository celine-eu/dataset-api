"""DPS data flows in the catalogue database (the one alembic migrates).

Two tables in the catalogue schema: `dps_data_flows` and `dps_control_planes`.
They have their own `MetaData` rather than the catalogue's declarative base, so
this feature stays in its package. `alembic/env.py` lists this metadata beside
the catalogue's, and revision `d7a2e5f19c40` creates the tables.

Why a database and not memory: a pull may reach any worker, and a flow must
outlive a restart. Every worker sees the same state, and
`SELECT … FOR UPDATE` serialises the signals for one flow across workers.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping, Optional

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Index,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from celine.dataset.core.config import get_settings
from celine.dataset.dps.flows import (
    ControlPlane,
    DataFlow,
    FlowConflict,
    FlowRole,
    FlowState,
)

metadata = MetaData(schema=get_settings().catalogue_schema)

data_flows = Table(
    "dps_data_flows",
    metadata,
    Column("id", String(255), primary_key=True),
    Column("role", String(16), nullable=False),
    Column("state", String(16), nullable=False),
    Column("caller", String(255), nullable=False),
    Column("participant_id", String(1024), nullable=False),
    Column("counter_party_id", String(1024), nullable=False),
    Column("agreement_id", String(255), nullable=False),
    Column("dataset_id", String(255), nullable=False),
    Column("transfer_type", String(512), nullable=False),
    Column("dataspace_context", String(255), nullable=True),
    Column("callback_address", Text, nullable=True),
    Column("claims", JSON, nullable=False),
    Column("labels", JSON, nullable=False),
    Column("metadata", JSON, nullable=False),
    # The provider's address is recorded without its credential.
    Column("data_address", JSON, nullable=True),
    # The live token's `jti`. Unique, so a token can never stand for two flows.
    Column("token_id", String(64), nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("token_id", name="uq_dps_data_flows_token_id"),
)

control_planes = Table(
    "dps_control_planes",
    metadata,
    Column("controlplane_id", String(255), primary_key=True),
    Column("endpoint", Text, nullable=False),
    Column("registered_by", String(255), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Index("ix_dps_control_planes_registered_by", "registered_by"),
)


def _row(flow: DataFlow) -> dict[str, Any]:
    return {
        "id": flow.id,
        "role": flow.role.value,
        "state": flow.state.value,
        "caller": flow.caller,
        "participant_id": flow.participant_id,
        "counter_party_id": flow.counter_party_id,
        "agreement_id": flow.agreement_id,
        "dataset_id": flow.dataset_id,
        "transfer_type": flow.transfer_type,
        "dataspace_context": flow.dataspace_context,
        "callback_address": flow.callback_address,
        "claims": flow.claims,
        "labels": flow.labels,
        "metadata": flow.metadata,
        "data_address": flow.data_address,
        "token_id": flow.token_id,
        "created_at": flow.created_at,
        "updated_at": flow.updated_at,
    }


def _flow(row: Mapping[str, Any]) -> DataFlow:
    return DataFlow(
        id=row["id"],
        role=FlowRole(row["role"]),
        state=FlowState(row["state"]),
        caller=row["caller"],
        participant_id=row["participant_id"],
        counter_party_id=row["counter_party_id"],
        agreement_id=row["agreement_id"],
        dataset_id=row["dataset_id"],
        transfer_type=row["transfer_type"],
        dataspace_context=row["dataspace_context"],
        callback_address=row["callback_address"],
        claims=row["claims"] or {},
        labels=row["labels"] or [],
        metadata=row["metadata"] or {},
        data_address=row["data_address"],
        token_id=row["token_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class SqlDataFlowRepository:
    """`DataFlowRepository` over the catalogue database. One short session per call."""

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]):
        self._sessionmaker = sessionmaker

    async def create(self, flow: DataFlow) -> None:
        async with self._sessionmaker() as session:
            try:
                await session.execute(insert(data_flows).values(**_row(flow)))
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                raise FlowConflict(f"data flow {flow.id} already exists") from exc

    async def get(self, flow_id: str) -> Optional[DataFlow]:
        return await self._one(data_flows.c.id == flow_id)

    async def by_token(self, token_id: str) -> Optional[DataFlow]:
        return await self._one(data_flows.c.token_id == token_id)

    @asynccontextmanager
    async def locked(self, flow_id: str) -> AsyncIterator[Optional[DataFlow]]:
        async with self._sessionmaker() as session:
            async with session.begin():
                row = (
                    await session.execute(
                        select(data_flows).where(data_flows.c.id == flow_id).with_for_update()
                    )
                ).mappings().first()
                flow = _flow(row) if row is not None else None
                yield flow
                if flow is not None:
                    values = _row(flow)
                    del values["id"], values["created_at"]
                    try:
                        await session.execute(
                            update(data_flows).where(data_flows.c.id == flow_id).values(**values)
                        )
                    except IntegrityError as exc:
                        raise FlowConflict("token id already bound to another data flow") from exc

    async def put_control_plane(self, cp: ControlPlane) -> bool:
        stmt = pg_insert(control_planes).values(
            controlplane_id=cp.controlplane_id,
            endpoint=cp.endpoint,
            registered_by=cp.registered_by,
            updated_at=datetime.now(timezone.utc),
        )
        # Replace only a registration the same caller owns: one statement, so
        # two callers cannot both pass a check and both write.
        stmt = stmt.on_conflict_do_update(
            index_elements=[control_planes.c.controlplane_id],
            set_={"endpoint": stmt.excluded.endpoint, "updated_at": stmt.excluded.updated_at},
            where=control_planes.c.registered_by == stmt.excluded.registered_by,
        )
        async with self._sessionmaker() as session:
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def delete_control_plane(self, controlplane_id: str, caller: str) -> bool:
        async with self._sessionmaker() as session:
            result = await session.execute(
                delete(control_planes).where(
                    control_planes.c.controlplane_id == controlplane_id,
                    control_planes.c.registered_by == caller,
                )
            )
            await session.commit()
            return result.rowcount == 1

    async def control_plane_registered_by(self, caller: str) -> Optional[ControlPlane]:
        async with self._sessionmaker() as session:
            row = (
                await session.execute(
                    select(control_planes)
                    .where(control_planes.c.registered_by == caller)
                    .order_by(control_planes.c.updated_at.desc())
                    .limit(1)
                )
            ).mappings().first()
        if row is None:
            return None
        return ControlPlane(
            controlplane_id=row["controlplane_id"],
            endpoint=row["endpoint"],
            registered_by=row["registered_by"],
        )

    async def _one(self, clause) -> Optional[DataFlow]:
        async with self._sessionmaker() as session:
            row = (await session.execute(select(data_flows).where(clause))).mappings().first()
        return _flow(row) if row is not None else None
