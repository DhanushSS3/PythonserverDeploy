"""Merge multiple heads

Revision ID: 6e566c344568
Revises: 395f98237d9e, e5a74d82bb22
Create Date: 2025-06-30 05:10:37.121554

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6e566c344568'
down_revision: Union[str, None] = ('395f98237d9e', 'e5a74d82bb22')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
