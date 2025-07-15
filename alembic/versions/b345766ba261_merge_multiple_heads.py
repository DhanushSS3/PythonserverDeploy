"""Merge multiple heads

Revision ID: b345766ba261
Revises: 5a4ddf849f0c
Create Date: 2025-07-14 22:05:24.213260

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b345766ba261'
down_revision: Union[str, None] = '5a4ddf849f0c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
