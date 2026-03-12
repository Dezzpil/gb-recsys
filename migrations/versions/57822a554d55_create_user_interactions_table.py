"""create user_interactions table

Revision ID: 57822a554d55
Revises: b5b2ab326ea5
Create Date: 2026-03-12 17:36:31.686875

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '57822a554d55'
down_revision: Union[str, None] = 'b5b2ab326ea5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'user_interactions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(), nullable=False),
        sa.Column('product_id', sa.Integer(), nullable=False),
        sa.Column('weight', sa.Float(), nullable=False),
        sa.Column('datetime', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_user_interactions_email', 'user_interactions', ['email'])


def downgrade() -> None:
    op.drop_table('user_interactions')
