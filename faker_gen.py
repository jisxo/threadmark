import os
import time
import random
import psycopg2
import traceback
from faker import Faker
from datetime import datetime, timedelta

from log.logger import common_logger as logger

fake = Faker()
db_host = os.getenv("DB_HOST", "localhost")

RED = "\033[91m"
RESET = "\033[0m"

# Connect to Database
conn = psycopg2.connect(
    host=db_host, 
    database="threadmark", 
    user="admin", 
    password="admin"
)
conn.autocommit = True
cursor = conn.cursor()

def create_order_transaction(customer_id, shop_id=None, status="PENDING", created_at=None, 
                             skip_payment=False, payment_status="SUCCESS", 
                             price_hijack=False):
    """Creates a full order transaction and allows injecting dirty data.

    Args:
        customer_id (int): Target customer ID.
        shop_id (int, optional): Target shop ID. Defaults to random (1-50).
        status (str, optional): Order status. Defaults to "PENDING".
        created_at (datetime, optional): Order timestamp. Defaults to now.
        skip_payment (bool, optional): Skip payment to make a 'Zombie Order'. Defaults to False.
        payment_status (str, optional): Payment status. Defaults to "SUCCESS".
        price_hijack (bool, optional): Force payment amount to $1.00. Defaults to False.

    Returns:
        int: Generated order_id if success, else None.
    """
    if not created_at:
        created_at = datetime.now()

    try:
        actual_shop_id = shop_id if shop_id else random.randint(1, 50)

        # 1. Insert Order
        cursor.execute(
            "INSERT INTO orders (customer_id, shop_id, status, created_at) VALUES (%s, %s, %s, %s) RETURNING order_id",
            (customer_id, actual_shop_id, status, created_at)
        )
        order_id = cursor.fetchone()[0]

        # 2. Insert Order Items
        product_id = random.randint(1, 5)
        quantity = random.randint(1, 2)
        unit_price = 10.00
        total_price = unit_price * quantity

        cursor.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, quantity, unit_price)
        )

        # 3. Insert Payment
        if not skip_payment:
            pay_amount = 1.00 if price_hijack else total_price
            cursor.execute(
                "INSERT INTO payments (order_id, payment_method, amount, status, created_at) VALUES (%s, %s, %s, %s, %s)",
                (order_id, 'CREDIT_CARD', pay_amount, payment_status, created_at)
            )

        # 4. Deduct Inventory
        cursor.execute(
            "UPDATE inventory SET stock_quantity = stock_quantity - %s, updated_at = %s WHERE product_id = %s",
            (quantity, datetime.now(), product_id)
        )

        return order_id

    except Exception:
        logger.error(f"[ERROR] Transaction failed: {traceback.format_exc()}")
        return None

def update_order_status(order_id, status):
    """Updates order status to simulate Out-of-Order scenarios.

    Args:
        order_id (int): Target order ID.
        status (str): New status (e.g., 'SHIPPED', 'DELIVERED').
    """
    cursor.execute(
        "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
        (status, datetime.now(), order_id)
    )

def run_scenario_engine(interval=2):
    """Runs infinite loop to generate normal and 8 dirty data scenarios randomly.

    Args:
        interval (int, optional): Sleep seconds between loops. Defaults to 2.
    """
    logger.info("[START] Scenario engine is running with V2 Schema... (8 Dirty Data Scenarios)")

    while True:
        dice = random.random()
        cust_id = random.randint(1, 10)

        try:
            # ----------------------------------------------------------------
            # Scenario 1: [LATE ARRIVAL] 
            # Network delay simulation (event_time is 10 minutes ago)
            # ----------------------------------------------------------------
            if dice < 0.10:
                past_time = datetime.now() - timedelta(minutes=10)
                create_order_transaction(cust_id, created_at=past_time)
                logger.info(f"[LATE_ARRIVAL] Order created with 10min delay for Customer: {cust_id}")

            # ----------------------------------------------------------------
            # Scenario 2: [DUPLICATE] 
            # Frontend double-click bug (same transaction sent twice)
            # ----------------------------------------------------------------
            elif dice < 0.20:
                create_order_transaction(cust_id)
                create_order_transaction(cust_id)
                logger.info(f"[DUPLICATE] Sent same order twice for Customer: {cust_id}")

            # ----------------------------------------------------------------
            # Scenario 3: [OUT-OF-ORDER] 
            # Async processing error (Status reversed: DELIVERED -> SHIPPED)
            # ----------------------------------------------------------------
            elif dice < 0.30:
                oid = create_order_transaction(cust_id)
                update_order_status(oid, "DELIVERED")
                time.sleep(1) # Wait 1 second
                update_order_status(oid, "SHIPPED") # Wrong past status arrives late
                logger.info(f"[OUT-OF-ORDER] Status reversed (DELIVERED -> SHIPPED) for Order: {oid}")

            # ----------------------------------------------------------------
            # Scenario 4: [BURST] 
            # Flash sale simulation (High traffic spike in a short time)
            # ----------------------------------------------------------------
            elif dice < 0.35:
                logger.info("[BURST] Flash sale starts! Generating 15 orders instantly.")
                for _ in range(15):
                    create_order_transaction(random.randint(1, 10))

            # ----------------------------------------------------------------
            # Scenario 5: [ZOMBIE ORDER] 
            # System crash during transaction (Order exists, but NO payment)
            # ----------------------------------------------------------------
            elif dice < 0.45:
                oid = create_order_transaction(cust_id, skip_payment=True)
                logger.info(f"[ZOMBIE_ORDER] Order {oid} created WITHOUT payment record.")

            # ----------------------------------------------------------------
            # Scenario 6: [OVERSELLING] 
            # Inventory lock failure (Stock drops below 0)
            # ----------------------------------------------------------------
            elif dice < 0.55:
                shop_id = random.randint(1, 50)
                # Force insert a massive order for Product 1
                cursor.execute(
                    "INSERT INTO orders (customer_id, shop_id, status) VALUES (%s, %s, 'PENDING') RETURNING order_id", 
                    (cust_id, shop_id)
                )
                oid = cursor.fetchone()[0]
                cursor.execute("INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, 1, 50, 10.00)", (oid,))
                cursor.execute("UPDATE inventory SET stock_quantity = stock_quantity - 50 WHERE product_id = 1")
                logger.info(f"[OVERSELLING] Massive stock deduction (-50) for Product 1 (Order: {oid}).")

            # ----------------------------------------------------------------
            # Scenario 7: [PRICE HIJACK] 
            # API manipulation (Order total is $10.00, but payment is $1.00)
            # ----------------------------------------------------------------
            elif dice < 0.65:
                oid = create_order_transaction(cust_id, price_hijack=True)
                logger.info(f"[PRICE_HIJACK] Price mismatch created for Order: {oid} (Paid $1.00)")

            # ----------------------------------------------------------------
            # Scenario 8: [SWAPPED SHOP] 
            # Race condition mapping error (Customer gets assigned an invalid shop_id)
            # ----------------------------------------------------------------
            elif dice < 0.75:
                # Assuming valid shops are 1-50, we inject a totally wrong shop_id (e.g., 999)
                invalid_shop_id = 999 
                oid = create_order_transaction(cust_id, shop_id=invalid_shop_id)
                # logger.info(f"[SWAPPED_SHOP] Race condition! Invalid shop_id {invalid_shop_id} assigned to Order: {oid}")
                logger.info(f"[{RED}SWAPPED_SHOP{RESET}] Race condition! Invalid shop_id {invalid_shop_id} assigned to Order: {oid}")

            # ----------------------------------------------------------------
            # NORMAL TRANSACTION
            # ----------------------------------------------------------------
            else:
                create_order_transaction(cust_id)
                logger.info(f"[NORMAL] Regular clean transaction for Customer: {cust_id}")

        except Exception:
            logger.error(f"[CRITICAL] Loop error: {traceback.format_exc()}")
        
        # Pause to prevent overloading the local Docker environment
        time.sleep(interval)

if __name__ == "__main__":
    run_scenario_engine(interval=2)