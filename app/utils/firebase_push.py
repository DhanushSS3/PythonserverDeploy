# app/utils/firebase_push.py

import datetime
import logging
from app.firebase_stream import firebase_db
from firebase_admin import firestore

def send_order_to_firebase(order, account_type: str = "live"):
    """
    Pushes full order data to Firebase under 'trade_data'.
    Works for both UserOrder and DemoUserOrder, but you should call it only for live users.
    """
    try:
        payload = {
            "order_id": order.order_id,
            "order_user_id": order.order_user_id,
            "order_company_name": order.order_company_name,
            "order_type": order.order_type,
            "order_status": order.order_status,
            "order_price": float(order.order_price),
            "order_quantity": float(order.order_quantity),
            "contract_value": float(order.contract_value),
            "margin": float(order.margin),

            "stop_loss": float(order.stop_loss) if order.stop_loss is not None else None,
            "take_profit": float(order.take_profit) if order.take_profit is not None else None,
            "close_price": float(order.close_price) if order.close_price is not None else None,
            "net_profit": float(order.net_profit) if order.net_profit is not None else None,
            "swap": float(order.swap or 0),
            "commission": float(order.commission or 0),

            "cancel_message": order.cancel_message,
            "close_message": order.close_message,

            "takeprofit_id": order.takeprofit_id,
            "stoploss_id": order.stoploss_id,
            "cancel_id": order.cancel_id,
            "close_id": order.close_id,
            "modify_id": order.modify_id,
            "stoploss_cancel_id": order.stoploss_cancel_id,
            "takeprofit_cancel_id": order.takeprofit_cancel_id,

            "timestamp": firestore.SERVER_TIMESTAMP,
            "account_type": account_type
        }

        firebase_ref = firebase_db.reference("trade_data")
        firebase_ref.push(payload)

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error pushing order to Firebase: {e}", exc_info=True)
