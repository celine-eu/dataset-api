"""add ontology_mapping to datasets_entries

Carries the resolved mapping spec with the catalogue entry rather than a
reference to it. `ontology_path` keeps the declaration (a shared spec name, or a
path relative to the governance.yaml); this holds what that resolved to, because
a pipeline-local spec lives in the pipelines checkout and the API serving
/vocabulary does not have that directory.

Revision ID: 8c1f4a90b7de
Revises: 4e2d7c63abb2
Create Date: 2026-08-09

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c1f4a90b7de'
down_revision: Union[str, Sequence[str], None] = '4e2d7c63abb2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# The schema name is written literally in 11ab075cfa8f, which created this table.
# Reading it from settings here would make the migration's target depend on the
# environment it runs in, so a deployment with a non-default CATALOGUE_SCHEMA
# would alter a table a previous migration never created.
SCHEMA = 'dataset_api'


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'datasets_entries',
        sa.Column('ontology_mapping', sa.JSON(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('datasets_entries', 'ontology_mapping', schema=SCHEMA)
