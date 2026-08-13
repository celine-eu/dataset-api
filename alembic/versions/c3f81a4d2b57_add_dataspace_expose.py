"""add dataspace_expose to datasets_entries

Splits the single exposure gate in two. `expose` keeps its meaning — listed in
the catalogue, served by `/catalogue*` and `/query`. `dataspace_expose` is new
and answers the other question the same boolean used to answer: is this dataset
*offered into the dataspace*, i.e. reachable by a request carrying EDR context.

**Backfilled to false for every existing row, deliberately.**

The obvious backfill is `dataspace_expose = expose`, and it would be wrong.
Until now `dataspace.expose` in a governance file was the only way a dataset
could appear in the catalogue at all, so every `true` in this table is a
statement about catalogue visibility — not a decision that the data belongs in a
dataspace. Copying it across would offer 60 datasets on the strength of a
statement that meant something else; 13 of them are CC-BY-NC-4.0 and 33 have no
declared licence. The governance files have been migrated to say `expose: true`
with `dataspace.expose: false`, so the next export writes false here anyway —
this backfill simply makes the window between migrating and re-exporting safe
rather than open.

Re-offering a dataset is one line in its governance.yaml plus a re-export.

Revision ID: c3f81a4d2b57
Revises: 8c1f4a90b7de
Create Date: 2026-08-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3f81a4d2b57"
down_revision: Union[str, Sequence[str], None] = "8c1f4a90b7de"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# The schema name is written literally in 11ab075cfa8f, which created this table.
# Reading it from settings here would make the migration's target depend on the
# environment it runs in, so a deployment with a non-default CATALOGUE_SCHEMA
# would alter a table a previous migration never created.
SCHEMA = "dataset_api"


def upgrade() -> None:
    """Upgrade schema."""
    # `server_default` rather than a follow-up UPDATE: it fills existing rows in
    # the same statement and keeps the column NOT NULL throughout, so there is no
    # instant where a row's dataspace offer is unknown. It also means a writer
    # running the previous code — which does not know this column — inserts a
    # withheld offer rather than failing.
    op.add_column(
        "datasets_entries",
        sa.Column(
            "dataspace_expose",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema=SCHEMA,
    )


def downgrade() -> None:
    """Downgrade schema.

    Dropping the column collapses the two gates back into one. Anything offered
    into the dataspace loses that record, and `expose` alone decides again.
    """
    op.drop_column("datasets_entries", "dataspace_expose", schema=SCHEMA)
