"""create merge_logs table

Revision ID: b5b2ab326ea5
Revises: 9fd14b8ab3a4
Create Date: 2026-03-12 15:18:39.450502

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5b2ab326ea5'
down_revision: Union[str, None] = '9fd14b8ab3a4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'merge_logs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('start_time', sa.DateTime(), nullable=False),
        sa.Column('duration', sa.Float(), nullable=False),
        sa.Column('metrika_file', sa.String(), nullable=False),
        sa.Column('orders_file', sa.String(), nullable=False),
        sa.Column('orders_records_count', sa.Integer(), nullable=False),
        sa.Column('unique_emails_count', sa.Integer(), nullable=False),
        sa.Column('unique_products_count', sa.Integer(), nullable=False),
        sa.Column('merge_time', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('merge_logs')
