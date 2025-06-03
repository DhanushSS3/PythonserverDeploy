print("helllo world")


import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.database.models import DemoUserOrder

async def print_demo_user_orders():
    print("Starting DB check...")  # Confirm function is running
    try:
        async with get_db() as db:
            print("Connected to DB, running query...")
            result = await db.execute("SELECT order_id FROM demo_user_orders")
            rows = result.fetchall()
            print("Order IDs in demo_user_orders:", rows)
    except Exception as e:
        print("Exception occurred:", e)

if __name__ == "__main__":
    print("Script started")
    asyncio.run(print_demo_user_orders())