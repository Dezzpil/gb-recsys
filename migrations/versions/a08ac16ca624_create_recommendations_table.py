"""create recommendations table

Revision ID: a08ac16ca624
Revises: c5088497088a
Create Date: 2026-03-12 17:48:21.136060

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a08ac16ca624'
down_revision: Union[str, None] = 'c5088497088a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'recommendations',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(), nullable=False),
        sa.Column('product_id', sa.Integer(), nullable=False),
        sa.Column('score', sa.Float(), nullable=False),
        sa.Column('model_name', sa.String(), nullable=False),
        sa.Column('merge_log_id', sa.Integer(), sa.ForeignKey('merge_logs.id'), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_recommendations_email', 'recommendations', ['email'])


def downgrade() -> None:
    op.drop_index('ix_recommendations_email', table_name='recommendations')
    op.drop_table('recommendations')
