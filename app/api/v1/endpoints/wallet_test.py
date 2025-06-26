"""
Test script to verify money request creation for withdrawal requests
Run this with: python -m app.api.v1.endpoints.wallet_test
"""

import asyncio
import httpx
import json
import sys
import os
from decimal import Decimal

# Add the root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

from app.database.session import get_db, engine
from app.database.models import MoneyRequest, User
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text

API_URL = "http://localhost:8000"  # Adjust if using a different URL
USER_TOKEN = "eyJhbGciOi...your_access_token_here..."  # Replace with a valid token for testing

async def test_create_withdraw_request():
    print("Testing withdrawal request creation...")
    
    # 1. Send a direct request using httpx to create a withdrawal
    headers = {"Authorization": f"Bearer {USER_TOKEN}"}
    data = {"amount": 100.0, "description": "Test withdrawal"}
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{API_URL}/users/wallet/withdraw",
                headers=headers,
                json=data,
                timeout=10.0
            )
            
        print(f"API Response Status: {response.status_code}")
        if response.status_code == 200:
            print(f"API Response Data: {response.json()}")
        else:
            print(f"API Response Error: {response.text}")
    except Exception as e:
        print(f"Error calling API: {str(e)}")
    
    # 2. Directly query the database
    print("\nChecking database for money requests...")
    try:
        async with AsyncSession(engine) as session:
            # Check for any money requests with type 'withdraw'
            result = await session.execute(
                select(MoneyRequest).filter(MoneyRequest.type == 'withdraw')
                .order_by(MoneyRequest.created_at.desc())
                .limit(5)
            )
            withdraw_requests = result.scalars().all()
            
            if withdraw_requests:
                print(f"Found {len(withdraw_requests)} withdrawal requests:")
                for req in withdraw_requests:
                    print(f"  ID: {req.id}, User ID: {req.user_id}, Amount: {req.amount}, Status: {req.status}, Created: {req.created_at}")
            else:
                print("No withdrawal requests found in database")
                
                # Check if there are any money requests at all
                result = await session.execute(
                    select(MoneyRequest)
                    .order_by(MoneyRequest.created_at.desc())
                    .limit(5)
                )
                all_requests = result.scalars().all()
                
                if all_requests:
                    print(f"Found {len(all_requests)} money requests (any type):")
                    for req in all_requests:
                        print(f"  ID: {req.id}, User ID: {req.user_id}, Type: {req.type}, Amount: {req.amount}")
                else:
                    print("No money requests found in database at all")
                
            # Check raw SQL to validate schema
            raw_result = await session.execute(text("SELECT * FROM money_requests LIMIT 1"))
            columns = raw_result.keys()
            print(f"\nMoney requests table columns: {columns}")
    except Exception as e:
        print(f"Database error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_create_withdraw_request()) 