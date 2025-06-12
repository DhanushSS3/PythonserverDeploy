"""Add user_favorite_symbols table

Revision ID: e5a74d82bb22
Revises: b43c66e9561c
Create Date: 2025-06-12 03:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision = 'e5a74d82bb22'
down_revision = 'b43c66e9561c'  # Set to the most recent migration ID
branch_labels = None
depends_on = None


def upgrade():
    """
    Creates the user_favorite_symbols table with appropriate columns and constraints.
    """
    op.create_table(
        'user_favorite_symbols',
        sa.Column('id', sa.Integer(), nullable=False, primary_key=True),
        sa.Column('user_id', sa.Integer(), nullable=False, index=True),
        sa.Column('symbol_id', sa.Integer(), nullable=False, index=True),
        sa.Column('user_type', sa.String(10), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=func.now(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=func.now(), onupdate=func.now(), nullable=False),
        sa.ForeignKeyConstraint(['symbol_id'], ['symbols.id'], ondelete='CASCADE'),
        sa.UniqueConstraint('user_id', 'symbol_id', 'user_type', name='_user_symbol_type_uc')
    )

    # Add index to improve query performance
    op.create_index('ix_user_favorite_symbols_user_id_symbol_id', 'user_favorite_symbols', ['user_id', 'symbol_id'])
    op.create_index('ix_user_favorite_symbols_user_type', 'user_favorite_symbols', ['user_type'])


def downgrade():
    """
    Drops the user_favorite_symbols table.
    """
    op.drop_index('ix_user_favorite_symbols_user_type', table_name='user_favorite_symbols')
    op.drop_index('ix_user_favorite_symbols_user_id_symbol_id', table_name='user_favorite_symbols')
    op.drop_table('user_favorite_symbols') 