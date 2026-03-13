"""create model_training_logs table

Revision ID: ddc969e8d3cb
Revises: 3f792b8b5ee7
Create Date: 2026-03-13 10:59:01.035030

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ddc969e8d3cb'
down_revision: Union[str, None] = '3f792b8b5ee7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'model_training_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('model_name', sa.String(), nullable=False),
        sa.Column('merge_log_id', sa.Integer(), sa.ForeignKey('merge_logs.id'), nullable=True),
        sa.Column('start_time', sa.DateTime(), nullable=False),
        sa.Column('duration', sa.Float(), nullable=False),
        sa.Column('recommendations_count', sa.Integer(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('model_training_logs')
