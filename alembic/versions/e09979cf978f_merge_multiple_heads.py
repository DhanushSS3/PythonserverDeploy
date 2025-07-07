"""Merge multiple heads

Revision ID: e09979cf978f
Revises: aaf89b3394dd
Create Date: 2025-07-07 03:08:50.485940

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e09979cf978f'
down_revision: Union[str, None] = 'aaf89b3394dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
