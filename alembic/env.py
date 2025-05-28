import asyncio
import os # Added for sys.path.append
import sys # Added for sys.path.append
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import AsyncEngine # Make sure AsyncEngine is imported

from alembic import context

# Add your project's root directory to the sys.path
# This is crucial so Alembic can find your 'app' package.
# Assuming 'alembic' folder is a sibling to your 'app' folder at the project root.
sys.path.append(os.path.abspath(".")) # <--- Ensure this line is present and correct for your project structure

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
from app.database.base import Base # <--- ENSURE THIS IMPORT PATH IS CORRECT for your Base declarative model

# !!! IMPORTANT: ADD THIS LINE !!!
# This ensures that all your SQLAlchemy models (and their table definitions)
# are registered with Base.metadata before Alembic tries to compare them.
import app.database.models # <--- ADD THIS LINE

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an actual DBAPI connection.

    By using this method, a script can be executed
    outside of a database connection.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

# ADD THIS NEW FUNCTION
def do_run_migrations(connection):
    """
    Synchronous function to configure Alembic context and run migrations.
    This function is called by AsyncConnection.run_sync().
    """
    context.configure(
        connection=connection, # This is the synchronous connection
        target_metadata=target_metadata,
        # You might need to add other configuration options here
        # For example, if you use schemas:
        # include_schemas=True,
        # For more accurate autogeneration on types:
        # compare_type=True,
        # For MySQL/MariaDB batch operations if needed for ALTER TABLE:
        # render_as_batch=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.
    """
    # This block gets the connectable (AsyncEngine)
    connectable = config.attributes.get("connection", None)
    if connectable is None:
        # Get section from alembic.ini, specifically for sqlalchemy.url
        alembic_cfg_section = config.get_section(config.config_ini_section, {})

        # Create AsyncEngine from config
        connectable = AsyncEngine(
            engine_from_config(
                alembic_cfg_section,
                prefix="sqlalchemy.",
                poolclass=pool.NullPool, # Use NullPool for Alembic to avoid connection issues
                future=True,
            )
        )

    async with connectable.connect() as connection:
        # Pass the AsyncConnection to the synchronous helper function
        # This will execute the migration logic with a synchronous connection
        # derived from the async one.
        await connection.run_sync(do_run_migrations)


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())