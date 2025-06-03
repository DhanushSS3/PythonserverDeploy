# app/core/firebase.py

import logging
import json
from typing import Dict, Any, Optional
from firebase_admin import db, firestore
import decimal
from datetime import datetime
from app.firebase_stream import firebase_db

logger = logging.getLogger(__name__)

async def send_order_to_firebase(order_data: Dict[str, Any], account_type: str = "live") -> bool:
    """
    Sends an order to Firebase Realtime Database under 'trade_data'.
    Works for both UserOrder and DemoUserOrder, but should be called only for live users.
    Returns True if successful, False otherwise.
    """
    try:
        payload = {
            "order_id": order_data.get('order_id'),
            "order_user_id": order_data.get('order_user_id'),
            "order_company_name": order_data.get('order_company_name'),
            "order_type": order_data.get('order_type'),
            "order_status": order_data.get('order_status'),
            "order_price": float(order_data.get('order_price', 0)),
            "order_quantity": float(order_data.get('order_quantity', 0)),
            "contract_value": float(order_data.get('contract_value', 0)),
            "margin": float(order_data.get('margin', 0)),

            "stop_loss": float(order_data.get('stop_loss')) if order_data.get('stop_loss') is not None else None,
            "take_profit": float(order_data.get('take_profit')) if order_data.get('take_profit') is not None else None,
            "close_price": float(order_data.get('close_price')) if order_data.get('close_price') is not None else None,
            "net_profit": float(order_data.get('net_profit')) if order_data.get('net_profit') is not None else None,
            "swap": float(order_data.get('swap', 0)),
            "commission": float(order_data.get('commission', 0)),

            "cancel_message": order_data.get('cancel_message'),
            "close_message": order_data.get('close_message'),

            "takeprofit_id": order_data.get('takeprofit_id'),
            "stoploss_id": order_data.get('stoploss_id'),
            "cancel_id": order_data.get('cancel_id'),
            "close_id": order_data.get('close_id'),
            "modify_id": order_data.get('modify_id'),
            "stoploss_cancel_id": order_data.get('stoploss_cancel_id'),
            "takeprofit_cancel_id": order_data.get('takeprofit_cancel_id'),

            "timestamp": firestore.SERVER_TIMESTAMP,
            "account_type": account_type
        }

        firebase_ref = firebase_db.reference("trade_data")
        firebase_ref.push(payload)
        logger.info(f"Order sent to Firebase successfully: {order_data.get('order_id')}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending order to Firebase: {e}", exc_info=True)
        return False

async def get_latest_market_data(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        ref = db.reference('datafeeds')
        if symbol:
            data = ref.child(symbol.upper()).get()
            return data
        else:
            data = ref.get()
            return data
    except Exception as e:
        logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
        return None

def get_latest_market_data_sync(symbol: str = None) -> Optional[Dict[str, Any]]:
    """
    Synchronous version of get_latest_market_data.
    Gets the latest market data from Firebase for a specific symbol or all symbols.
    Returns None if data is not available.
    """
    try:
        ref = db.reference('datafeeds')
        if symbol:
            data = ref.child(symbol.upper()).get()
            return data
        else:
            data = ref.get()
            return data
    except Exception as e:
        logger.error(f"Error getting market data from Firebase: {e}", exc_info=True)
        return None 