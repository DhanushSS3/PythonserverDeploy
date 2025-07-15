"""initialize_id_counter_table

Revision ID: 5a4ddf849f0c
Revises: fba9d1ecf60e
Create Date: 2025-07-14 21:32:26.176758

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5a4ddf849f0c'
down_revision: Union[str, None] = 'fba9d1ecf60e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Initialize the id_counter table with the required record
    # This ensures the table has the initial counter record needed by generate_unique_10_digit_id
    connection = op.get_bind()
    
    # Check if the record already exists to avoid duplicate key errors
    result = connection.execute(sa.text("SELECT COUNT(*) FROM id_counter WHERE id = 1"))
    count = result.scalar()
    
    if count == 0:
        # Insert the initial record with id=1 and last_value=0
        connection.execute(sa.text("INSERT INTO id_counter (id, last_value) VALUES (1, 0)"))
        connection.commit()


def downgrade() -> None:
    """Downgrade schema."""
    # Remove the initial record if needed
    connection = op.get_bind()
    connection.execute(sa.text("DELETE FROM id_counter WHERE id = 1"))
    connection.commit()
