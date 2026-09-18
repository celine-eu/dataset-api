"""add the DPS data plane tables

`dps_data_flows` holds the Data Plane Signaling flows this service answers for,
and the `jti` of each flow's one live pull token. `dps_control_planes` holds the
spec's control-plane registrations, without their authorization profile.

Both live in the catalogue database because the data plane is part of this
service. A pull may reach any worker, so memory is not enough, and a flow must
outlive a restart. See `docs/dps-data-plane.md`.

Revision ID: d7a2e5f19c40
Revises: c3f81a4d2b57
Create Date: 2026-09-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d7a2e5f19c40"
down_revision: Union[str, Sequence[str], None] = "c3f81a4d2b57"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Written literally, as in c3f81a4d2b57: a migration's target must not depend on
# the environment it runs in.
SCHEMA = "dataset_api"


def upgrade() -> None:
    op.create_table(
        "dps_data_flows",
        sa.Column("id", sa.String(length=255), primary_key=True),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("caller", sa.String(length=255), nullable=False),
        sa.Column("participant_id", sa.String(length=1024), nullable=False),
        sa.Column("counter_party_id", sa.String(length=1024), nullable=False),
        sa.Column("agreement_id", sa.String(length=255), nullable=False),
        sa.Column("dataset_id", sa.String(length=255), nullable=False),
        sa.Column("transfer_type", sa.String(length=512), nullable=False),
        sa.Column("dataspace_context", sa.String(length=255), nullable=True),
        sa.Column("callback_address", sa.Text(), nullable=True),
        sa.Column("claims", sa.JSON(), nullable=False),
        sa.Column("labels", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("data_address", sa.JSON(), nullable=True),
        sa.Column("token_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("token_id", name="uq_dps_data_flows_token_id"),
        schema=SCHEMA,
    )
    op.create_table(
        "dps_control_planes",
        sa.Column("controlplane_id", sa.String(length=255), primary_key=True),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("registered_by", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dps_control_planes_registered_by",
        "dps_control_planes",
        ["registered_by"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    """Drops every DPS flow: live pull tokens stop working."""
    op.drop_index("ix_dps_control_planes_registered_by", table_name="dps_control_planes", schema=SCHEMA)
    op.drop_table("dps_control_planes", schema=SCHEMA)
    op.drop_table("dps_data_flows", schema=SCHEMA)
