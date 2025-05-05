# scripts/create_admin.py

import asyncio
import os
import sys
import datetime
from decimal import Decimal # Import Decimal for financial fields

# Add your project root to the sys.path so you can import your modules
# Assuming this script is in a 'scripts' directory at the project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import necessary components from your application
# Correct import: Import AsyncSessionLocal, not async_session
from app.database.session import engine, AsyncSessionLocal # Import engine and AsyncSessionLocal
# Import Base and User model from your models file
from app.database.models import Base, User # Import Base and User model
from app.core.security import get_password_hash # Import password hashing utility
from app.core.config import get_settings # Import settings
from sqlalchemy.future import select # Import select for database query

async def create_initial_admin():
    """
    Creates the initial administrator user if one does not exist.
    Reads admin credentials from environment variables.
    """
    settings = get_settings()

    # Define admin credentials (get these securely from environment variables)
    # These environment variables MUST be set when running the script in production.
    admin_email = os.getenv("INITIAL_ADMIN_EMAIL")
    admin_password = os.getenv("INITIAL_ADMIN_PASSWORD")
    admin_name = os.getenv("INITIAL_ADMIN_NAME", "Site Administrator") # Optional env var, defaults if not set

    if not admin_email:
        print("ERROR: INITIAL_ADMIN_EMAIL environment variable must be set.")
        sys.exit(1)
    if not admin_password:
        print("ERROR: INITIAL_ADMIN_PASSWORD environment variable must be set.")
        sys.exit(1)

    # Use AsyncSessionLocal to get a new session instance
    async with AsyncSessionLocal() as db:
        async with db.begin(): # Use a transaction for atomicity
            # Check if an admin user already exists by email
            existing_admin_query = await db.execute(
                select(User).filter(User.email == admin_email)
            )
            existing_admin = existing_admin_query.scalars().first()

            if existing_admin:
                print(f"Admin user with email {admin_email} already exists (ID: {existing_admin.id}). Skipping creation.")
                return

            # Hash the password
            hashed_password = get_password_hash(admin_password)

            # Create the admin user
            admin_user = User(
                name=admin_name,
                email=admin_email,
                phone_number="N/A", # Provide a default or get from env var if needed
                hashed_password=hashed_password,
                user_type="admin", # Set the user type to 'admin'
                city="N/A", # Provide defaults for required fields
                state="N/A",
                pincode=0, # Provide a default
                wallet_balance=Decimal("0.00"), # Provide default Decimal values
                leverage=Decimal("0.0"),
                margin=Decimal("0.0"),
                status=1, # Set status to 1 (e.g., verified/active)
                isActive=1, # Set isActive to 1 (e.g., active)
                # Other nullable fields will default to None
                # Timestamps will be set by the database default (func.now())
            )

            db.add(admin_user)
            # No need for db.flush() unless you need the ID immediately after adding
            # No need for db.refresh() unless you need default values populated immediately

            print(f"\nInitial admin user created successfully with email: {admin_email}")
            # The user ID will be available after the commit and refresh if needed
            # print(f"User ID: {admin_user.id}") # Uncomment if you refresh

            print("\nIMPORTANT: Keep the admin email and password secure!")


async def main():
    """
    Main function to run the admin creation process.
    Optionally includes database table creation for initial setup.
    """
    # In a real production deployment, database migrations (like Alembic)
    # should handle table creation and schema updates.
    # The code below is useful for initial development setup.
    # print("Checking/Creating database tables (if they don't exist)...")
    # async with engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.create_all)
    # print("Database tables checked/created.")

    await create_initial_admin()

if __name__ == "__main__":
    # How to run this script:
    # 1. Activate your Python virtual environment.
    # 2. Set environment variables for admin credentials:
    #    On Windows PowerShell: $env:INITIAL_ADMIN_EMAIL="your.admin@example.com"; $env:INITIAL_ADMIN_PASSWORD="your_secure_password"
    #    On Linux/macOS or Git Bash: export INITIAL_ADMIN_EMAIL="your.admin@example.com"; export INITIAL_ADMIN_PASSWORD="your_secure_password"
    #    On Windows Command Prompt (cmd.exe): set INITIAL_ADMIN_EMAIL=... & set INITIAL_ADMIN_PASSWORD=...
    # 3. Run the script from your project root directory: python scripts/create_admin.py

    asyncio.run(main())
