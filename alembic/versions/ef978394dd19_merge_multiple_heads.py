"""Merge multiple heads

Revision ID: ef978394dd19
Revises: d0b466401f7a
Create Date: 2025-06-30 22:11:50.706773

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ef978394dd19'
down_revision: Union[str, None] = 'd0b466401f7a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
