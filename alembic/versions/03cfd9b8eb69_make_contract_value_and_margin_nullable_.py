"""make contract_value and margin nullable in order models and action_type required in OrderActionHistory

Revision ID: 03cfd9b8eb69
Revises: d9bc96abf326
Create Date: 2025-06-10 21:49:05.851583

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision: str = '03cfd9b8eb69'
down_revision: Union[str, None] = 'd9bc96abf326'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Make contract_value and margin nullable in order tables
    op.alter_column('user_orders', 'contract_value',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    op.alter_column('user_orders', 'margin',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    
    op.alter_column('demo_user_orders', 'contract_value',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    op.alter_column('demo_user_orders', 'margin',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    
    op.alter_column('rock_user_orders', 'contract_value',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    op.alter_column('rock_user_orders', 'margin',
               existing_type=mysql.DECIMAL(precision=18, scale=8),
               nullable=True)
    
    # Make action_type required in OrderActionHistory
    # First update existing NULL values to 'UNKNOWN'
    op.execute("UPDATE order_action_history SET action_type = 'UNKNOWN' WHERE action_type IS NULL")
    
    # Then make the column not nullable
    op.alter_column('order_action_history', 'action_type',
               existing_type=sa.String(length=50),
               nullable=False)


def downgrade() -> None:
    """Downgrade schema."""
    # Revert action_type to nullable in OrderActionHistory
    op.alter_column('order_action_history', 'action_type',
               existing_type=sa.String(length=50),
               nullable=True)
    
    # Note: We can't safely revert the contract_value and margin columns 
    # to NOT NULL because existing records might have NULL values.
    # If needed, you would need to update those NULL values before making
    # the columns NOT NULL again.
