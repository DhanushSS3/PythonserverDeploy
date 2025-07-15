"""Merge multiple heads

Revision ID: a857b74cd7a5
Revises: ac9ae41fd5c5
Create Date: 2025-07-14 21:10:38.084967

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a857b74cd7a5'
down_revision: Union[str, None] = 'ac9ae41fd5c5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
