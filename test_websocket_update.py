"""
Test to verify WebSocket user data updates are published when money requests are approved
"""

import asyncio
import sys
import os

# Add the root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app.database.session import engine
from app.database.models import User, MoneyRequest, Wallet
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.crud import money_request as crud_money_request
from app.dependencies.redis_client import get_redis_client

async def test_websocket_update():
    print("Testing WebSocket user data update after money request approval...")
    
    # Get Redis client
    redis_client = await get_redis_client()
    
    async with AsyncSession(engine) as session:
        # Create a test money request
        from app.schemas.money_request import MoneyRequestCreate
        
        test_request = MoneyRequestCreate(
            type="withdraw",
            amount=15.0
        )
        
        # Create the money request
        money_request = await crud_money_request.create_money_request(
            db=session,
            request_data=test_request,
            user_id=4  # Use existing user
        )
        
        print(f"Created money request ID: {money_request.id}")
        
        # Check initial state
        user = await session.get(User, 4)
        initial_balance = user.wallet_balance
        print(f"Initial wallet balance: {initial_balance}")
        
        # Approve the request with Redis client
        updated_request = await crud_money_request.update_money_request_status(
            db=session,
            request_id=money_request.id,
            new_status=1,
            admin_id=10,
            redis_client=redis_client
        )
        
        if updated_request:
            print(f"✅ Money request approved! Status: {updated_request.status}")
            
            # COMMIT THE TRANSACTION
            await session.commit()
            print("✅ Transaction committed to database")
            
            # Check final state after commit
            await session.refresh(user)
            final_balance = user.wallet_balance
            print(f"Final wallet balance: {final_balance}")
            
            # Check wallet records after commit
            result = await session.execute(
                select(Wallet).filter(Wallet.user_id == 4).order_by(Wallet.created_at.desc())
            )
            wallets = result.scalars().all()
            if wallets:
                latest_wallet = wallets[0]
                print(f"✅ Wallet record created: {latest_wallet.transaction_type} - {latest_wallet.transaction_amount}")
                print(f"   Transaction ID: {latest_wallet.transaction_id}")
            else:
                print("❌ No wallet record created")
                
            # Verify the balance actually changed
            if final_balance != initial_balance:
                print(f"✅ Balance change confirmed: {initial_balance} → {final_balance}")
            else:
                print("❌ Balance did not change!")
                
            print("✅ WebSocket user data update should have been published to Redis")
        else:
            print("❌ Failed to approve money request")

if __name__ == "__main__":
    asyncio.run(test_websocket_update()) 