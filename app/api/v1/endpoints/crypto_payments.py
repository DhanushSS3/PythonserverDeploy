from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Any
import httpx
import hmac
import hashlib
import json
from uuid import uuid4

from app.database.session import get_db
from app.core.config import settings
from app.core.security import get_current_user
from app.database.models import User, DemoUser, CryptoPayment
from app.schemas.crypto_payment import PaymentRequest, PaymentResponse, CurrencyListResponse, CallbackData
from app.crud.crypto_payment import create_payment_record, get_payment_by_merchant_order_id, update_payment_status


router = APIRouter()

@router.post("/generate-payment-url", response_model=PaymentResponse)
async def generate_payment_url(
    request: PaymentRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    merchant_order_id = f'livefx_{uuid4().hex}'
    
    request_body = {
        'merchantOrderId': merchant_order_id,
        'baseAmount': str(request.baseAmount),
        'baseCurrency': request.baseCurrency,
        'settledCurrency': request.settledCurrency,
        'networkSymbol': request.networkSymbol,
        'callBackUrl': 'https://api.livefxhub.com/api/cryptoCallback' # This should be configurable
    }

    raw = json.dumps(request_body, separators=(',', ':'), ensure_ascii=False)
    signature = hmac.new(
        bytes(settings.TYLT_API_SECRET, 'utf-8'),
        msg=bytes(raw, 'utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()

    headers = {
        'X-TLP-APIKEY': settings.TYLT_API_KEY,
        'X-TLP-SIGNATURE': signature,
        'Content-Type': 'application/json',
    }

    async with httpx.AsyncClient() as client:
        try:
            res = await client.post(
                'https://api.tylt.money/transactions/merchant/createPayinRequest',
                headers=headers,
                json=request_body
            )
            res.raise_for_status()

            # Create payment record before returning response
            await create_payment_record(db, current_user.id, merchant_order_id, request.dict())

            tylt_data = res.json().get("data", res.json())
            payment_response_data = {
                "paymentUrl": tylt_data.get("paymentURL"),
                "merchantOrderId": tylt_data.get("merchantOrderId"),
                # Add more fields if your schema expects them
            }

            return {
                "status": True,
                "message": "PaymentUrl Generated Successfully",
                "data": payment_response_data
            }
        except httpx.HTTPStatusError as e:
            return {
                "status": False,
                "message": "Failed to generate PaymentUrl",
                "error": e.response.text
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router.get("/currency-list", response_model=CurrencyListResponse)
async def currency_list(current_user: User = Depends(get_current_user)):
    request_body = {}
    raw = json.dumps(request_body)

    signature = hmac.new(
        bytes(settings.TYLT_API_SECRET, 'utf-8'),
        msg=bytes(raw, 'utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()

    headers = {
        'X-TLP-APIKEY': settings.TYLT_API_KEY,
        'X-TLP-SIGNATURE': signature,
        'Content-Type': 'application/json',
    }

    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(
                'https://api.tylt.money/transactions/merchant/getSupportedCryptoCurrenciesList',
                headers=headers
            )
            res.raise_for_status()
            return {
                "status": True,
                "message": "Data Fetched Successfully",
                "data": res.json()
            }
        except httpx.HTTPStatusError as e:
            return {
                "status": False,
                "message": "Failed to fetch data",
                "error": e.response.text
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@router.post("/crypto-callback")
async def crypto_callback(request: Request, db: AsyncSession = Depends(get_db)):
    tlp_signature = request.headers.get('x-tlp-signature')
    raw_body = await request.body()

    calculated_hmac = hmac.new(
        bytes(settings.TYLT_API_SECRET, 'utf-8'),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(calculated_hmac, tlp_signature):
        raise HTTPException(status_code=400, detail="Invalid HMAC signature")

    data = await request.json()
    
    if data.get('type') == 'pay-in' and data.get('status') == 'completed':
        merchant_order_id = data.get('merchantOrderId')
        payment = await get_payment_by_merchant_order_id(db, merchant_order_id)
        
        if payment and payment.status == 'PENDING':
            await update_payment_status(db, payment, 'COMPLETED', data)
            
            # This is where you would update the user's wallet
            # For now, we'll just log it. A proper wallet update needs a dedicated function.
            # Example: await add_funds_to_wallet(db, payment.user_id, payment.base_amount)
            print(f"User {payment.user_id} wallet updated with {payment.base_amount} {payment.base_currency}")

    return {"status": "ok"} 