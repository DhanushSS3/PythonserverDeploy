"""Merge multiple heads

Revision ID: 8a0e8798b4be
Revises: c90ff05850ab
Create Date: 2025-06-30 22:07:52.777454

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a0e8798b4be'
down_revision: Union[str, None] = 'c90ff05850ab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
