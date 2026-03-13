"""create steam cache table

Revision ID: 61ccd299cb2e
Revises: ddc969e8d3cb
Create Date: 2026-03-13 15:43:38.762811

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '61ccd299cb2e'
down_revision: Union[str, None] = 'ddc969e8d3cb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'steam_cache',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('game_name', sa.String(), nullable=False),
        sa.Column('similar_games', sa.JSON(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('game_name')
    )
    op.create_index('ix_steam_cache_game_name', 'steam_cache', ['game_name'])


def downgrade() -> None:
    op.drop_index('ix_steam_cache_game_name', table_name='steam_cache')
    op.drop_table('steam_cache')
