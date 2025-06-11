"""add order_id to Wallet model

Revision ID: d5a74d82aa11
Revises: 6378d1e41f00
Create Date: 2025-06-10 22:05:17.558161

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5a74d82aa11'
down_revision: Union[str, None] = '6378d1e41f00'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add order_id column to wallets table
    op.add_column('wallets', sa.Column('order_id', sa.String(length=64), nullable=True))
    op.create_index(op.f('ix_wallets_order_id'), 'wallets', ['order_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Remove order_id column from wallets table
    op.drop_index(op.f('ix_wallets_order_id'), table_name='wallets')
    op.drop_column('wallets', 'order_id')
