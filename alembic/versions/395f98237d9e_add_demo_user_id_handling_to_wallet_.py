"""add demo_user_id handling to wallet endpoints

Revision ID: 395f98237d9e
Revises: d5a74d82aa11
Create Date: 2025-06-10 22:16:56.551267

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '395f98237d9e'
down_revision: Union[str, None] = 'd5a74d82aa11'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
