"""Merge multiple heads

Revision ID: 40804000f6ee
Revises: beca26b3e8b6
Create Date: 2025-06-30 22:33:10.063741

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '40804000f6ee'
down_revision: Union[str, None] = 'beca26b3e8b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
