import sqlite3
import json
from datetime import datetime


def save_order_info_db(order, balance_data, position):
    # Extract order details
    print(f"Order Details: {order}")

    # Prepare order data in the desired format
    order_data_formatted = {
        order["symbol"]: {
            "quantity": float(order["origQty"]),  # 거래 완료된 수량
            "status": order["status"],  # 거래 상태
            "side": order["side"],  # 거래 방향
            "closePosition": order["closePosition"],  # 청산 여부
        }
    }
    order_data_serialized = json.dumps(order_data_formatted)

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    balance_amount = (
        float(balance_data["balance"])
        if balance_data and "balance" in balance_data
        else 0
    )

    try:
        # Connect to SQLite database
        conn = sqlite3.connect("/home/hanvit4303/dk_tradebot/server/future.db")
        cursor = conn.cursor()

        # Serialize order data

        # Insert data into the table
        cursor.execute(
            """
            INSERT INTO future (name, date, ASSET, SYMBOL, ROI, OrderData)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "DK",
                current_time,
                balance_amount,
                position,
                0,
                order_data_serialized,
            ),
        )

        # Commit the transaction
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQLite Error: {e}")
    finally:
        # Close the connection
        print("Order successfully inserted into database.")
        if conn:
            conn.close()


def insert_duplicate_of_last_row():
    db_path = "/home/hanvit4303/dk_tradebot/server/future.db"

    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Fetch the last row data
        cursor.execute("SELECT * FROM future ORDER BY id DESC LIMIT 1")
        last_row = cursor.fetchone()

        if last_row is not None:
            # Insert the same data into a new row
            cursor.execute(
                """
                INSERT INTO future (name, date, ASSET, SYMBOL, ROI, OrderData)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                last_row[1:],  # Exclude the id field
            )

            # Commit the transaction
            conn.commit()
            print("Duplicate row successfully inserted into database.")
        else:
            print("No rows found in the database.")

    except sqlite3.Error as e:
        print(f"SQLite Error: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()


# Usage
