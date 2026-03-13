"""remove interactions_count from merge_logs

Revision ID: 3f792b8b5ee7
Revises: f4c82aa8391a
Create Date: 2026-03-13 09:08:41.456968

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3f792b8b5ee7'
down_revision: Union[str, None] = 'f4c82aa8391a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column('merge_logs', 'interactions_count')


def downgrade() -> None:
    op.add_column('merge_logs', sa.Column('interactions_count', sa.Integer(), nullable=True))
