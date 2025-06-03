from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_db
from app.database.models import DemoUserOrder

async def print_demo_user_orders():
    async with get_db() as db:
        result = await db.execute("SELECT order_id FROM demo_user_orders")
        rows = result.fetchall()
        print("Order IDs in demo_user_orders:", rows)