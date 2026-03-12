"""add_merge_log_id_to_user_interactions

Revision ID: c5088497088a
Revises: 57822a554d55
Create Date: 2026-03-12 17:40:00.274170

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c5088497088a'
down_revision: Union[str, None] = '57822a554d55'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('user_interactions', sa.Column('merge_log_id', sa.Integer(), nullable=True))
    op.create_foreign_key(
        'fk_user_interactions_merge_log_id_merge_logs',
        'user_interactions', 'merge_logs',
        ['merge_log_id'], ['id']
    )


def downgrade() -> None:
    op.drop_constraint('fk_user_interactions_merge_log_id_merge_logs', 'user_interactions', type_='foreignkey')
    op.drop_column('user_interactions', 'merge_log_id')
