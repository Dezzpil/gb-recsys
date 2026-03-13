"""add interactions_count to merge_logs

Revision ID: f4c82aa8391a
Revises: a08ac16ca624
Create Date: 2026-03-13 08:57:37.362811

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f4c82aa8391a'
down_revision: Union[str, None] = 'a08ac16ca624'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('merge_logs', sa.Column('interactions_count', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('merge_logs', 'interactions_count')
