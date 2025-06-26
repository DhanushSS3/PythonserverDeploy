"""
Script to create the first admin user directly in the database
Run with: python create_admin.py
"""

import asyncio
import sys
import os
from decimal import Decimal
from datetime import datetime
from sqlalchemy import text, select

# Add the root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app.database.session import engine
from app.database.models import User
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.security import get_password_hash
from app.crud.user import generate_unique_account_number

async def create_first_admin():
    print("Creating first admin user...")
    
    # Admin user details - modify these as needed
    admin_data = {
        "name": "Super Admin",
        "email": "admin@example.com",
        "phone_number": "1234567890",
        "password": "admin123456",  # Change this to a secure password
        "country": "India",
        "city": "Mumbai",
        "state": "Maharashtra",
        "pincode": 400001,
        "group_name": "default",
        "bank_account_number": "1234567890",
        "bank_ifsc_code": "ABCD0001234",
        "bank_holder_name": "Super Admin",
        "bank_branch_name": "Main Branch",
        "security_question": "What is your favorite color?",
        "security_answer": "blue",
        "address_proof": "Aadhar Card",
        "id_proof": "PAN Card",
        "is_self_trading": 1,
        "fund_manager": None,
        "user_type": "admin",
        "isActive": 1
    }
    
    try:
        async with AsyncSession(engine) as session:
            # Check if admin already exists using proper SQLAlchemy syntax
            result = await session.execute(
                select(User.id).filter(User.user_type == "admin").limit(1)
            )
            if result.scalar():
                print("Admin user already exists!")
                return
            
            # Generate unique account number
            account_number = await generate_unique_account_number(session)
            
            # Hash password
            hashed_password = get_password_hash(admin_data["password"])
            
            # Create admin user
            admin_user = User(
                name=admin_data["name"],
                email=admin_data["email"],
                phone_number=admin_data["phone_number"],
                hashed_password=hashed_password,
                country=admin_data["country"],
                city=admin_data["city"],
                state=admin_data["state"],
                pincode=admin_data["pincode"],
                group_name=admin_data["group_name"],
                bank_account_number=admin_data["bank_account_number"],
                bank_ifsc_code=admin_data["bank_ifsc_code"],
                bank_holder_name=admin_data["bank_holder_name"],
                bank_branch_name=admin_data["bank_branch_name"],
                security_question=admin_data["security_question"],
                security_answer=admin_data["security_answer"],
                address_proof=admin_data["address_proof"],
                id_proof=admin_data["id_proof"],
                is_self_trading=admin_data["is_self_trading"],
                fund_manager=admin_data["fund_manager"],
                user_type=admin_data["user_type"],
                wallet_balance=Decimal("0.0"),
                margin=Decimal("0.0"),
                leverage=Decimal("100.0"),
                status=1,
                isActive=admin_data["isActive"],
                account_number=account_number,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(admin_user)
            await session.commit()
            await session.refresh(admin_user)
            
            print(f"✅ Admin user created successfully!")
            print(f"   ID: {admin_user.id}")
            print(f"   Email: {admin_user.email}")
            print(f"   Password: {admin_data['password']}")
            print(f"   Account Number: {admin_user.account_number}")
            print(f"   User Type: {admin_user.user_type}")
            print("\n⚠️  IMPORTANT: Change the password after first login!")
            
    except Exception as e:
        print(f"❌ Error creating admin user: {str(e)}")
        raise

if __name__ == "__main__":
    print("=== Create First Admin User ===")
    print("This script will create the first admin user in the database.")
    print("Make sure to change the default password after creation!\n")
    
    # Ask for confirmation
    response = input("Do you want to continue? (y/N): ")
    if response.lower() in ['y', 'yes']:
        asyncio.run(create_first_admin())
    else:
        print("Admin creation cancelled.") 