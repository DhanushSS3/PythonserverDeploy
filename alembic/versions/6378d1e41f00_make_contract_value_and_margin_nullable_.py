"""make contract_value and margin nullable in order models and action_type required in OrderActionHistory

Revision ID: 6378d1e41f00
Revises: 03cfd9b8eb69
Create Date: 2025-06-10 21:50:26.156010

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6378d1e41f00'
down_revision: Union[str, None] = '03cfd9b8eb69'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
