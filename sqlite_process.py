import sqlite3
import json
from datetime import datetime
import os


def update_balances_in_db(api_num, account, init_asset):
    # Fetch the new USDT balance after the trade
    new_usdt_balance = account.get_api_balance(api_num)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()
        print(f"새로운 USDT_API_{api_num}열에 {init_asset}을 삽입합니다.")
        # print(f"새로운 ASSET_API_{api_num}열에 {init_asset}을 삽입합니다.")
        if api_num == 1:
            cursor.execute(
                """
                INSERT INTO trades (name, date, USDT_API_1, symbol_API_1, ROI_API_1, ASSET_API_1, USDT_API_2, symbol_API_2, ROI_API_2, ASSET_API_2, ASSET)
                SELECT ?, ?, ?, symbol_API_1, ROI_API_1, ASSET_API_1, USDT_API_2, symbol_API_2, ROI_API_2, ASSET_API_2, ASSET FROM trades ORDER BY id DESC LIMIT 1
                """,
                (
                    "DK",
                    current_time,
                    new_usdt_balance,
                ),
            )
        elif api_num == 2:
            cursor.execute(
                """
                UPDATE trades
                SET USDT_API_2 = ?, date = ?, name = ?
                WHERE id = (SELECT id FROM trades ORDER BY id DESC LIMIT 1)
                """,
                (new_usdt_balance, current_time, "DK"),
            )

        conn.commit()


def update_after_buy(api_num, symbol, amount_bought, account, current_price):
    """
    Update the database after buying a symbol.

    :param api_num: The API number (1 or 2).
    :param symbol: The symbol that was bought.
    :param amount_bought: The amount of the symbol that was bought.
    """
    # Fetch the new USDT balance and update symbol holdings
    asset_usdt_balance = account.get_api_balance(api_num)  # 42
    new_symbol_holdings = get_updated_holdings(api_num, symbol, amount_bought, account)
    """{'TRB': 10.935, 'CRVUSDT': 16.6} new_symbol_holdings"""
    # print(f"{new_symbol_holdings} current_holdings")
    # Database column names

    # Connect to the SQLite database
    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # 마지막 행의 데이터를 조회합니다
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()

        # API에 따라 현재 잔액과 총 자산을 가져옵니다
        current_usdt_balance = row[f"USDT_API_{api_num}"]
        current_asset_value = row[f"ASSET_API_{api_num}"]
        # 현재 보유 코인 정보를 가져옵니다

        # 코인심볼과 코인의 양
        coin_asset = amount_bought * current_price
        current_usdt_balance -= coin_asset
        # 현재 보유 코인 정보 업데이트
        symbol_holdings_str = row[f"symbol_API_{api_num}"]
        current_symbol_holdings = (
            json.loads(symbol_holdings_str) if symbol_holdings_str else {}
        )
        current_symbol_holdings[symbol] = (
            current_symbol_holdings.get(symbol, 0) + amount_bought
        )

        total_coin_value = 0  # 모든 코인의 가치 합계
        for coin, quantity in current_symbol_holdings.items():
            if coin == symbol:
                coin_value = quantity * current_price  # 구매한 코인의 현재 가치
            else:
                coin_value = quantity * account.get_current_price(
                    coin
                )  # 기타 보유 코인의 현재 가치
            total_coin_value += coin_value
        print(f"{current_usdt_balance}current_usdt_balance")
        current_asset_value = total_coin_value + current_usdt_balance  # 최종 자산 가치

        cursor.execute(
            f"""
            UPDATE trades
            SET USDT_API_{api_num} = ?, ASSET_API_{api_num} = ?, symbol_API_{api_num} = ?
            WHERE id = ?
            """,
            (
                current_usdt_balance,
                current_asset_value,
                json.dumps(current_symbol_holdings),
                row["id"],
            ),
        )
        # 업데이트된 값 출력
        border = "+" + "-" * 45 + "BUY" + "-" * 45 + "+"
        print(border)
        print(f"USDT_API_{api_num} updated to: {current_usdt_balance}")
        print(f"ASSET_API_{api_num} updated to: {current_asset_value}")
        print(f"symbol_API_{api_num} updated to: {json.dumps(current_symbol_holdings)}")

        conn.commit()


def get_updated_holdings(api_num, symbol, amount_bought, account):
    # 현재 보유 코인 정보를 업데이트합니다
    current_holdings = account.get_api_holdings(api_num)
    current_holdings[symbol] = current_holdings.get(symbol, 0) + amount_bought
    return current_holdings


def update_after_sell(api_num, symbol, amount_sold, account, current_price):
    """
    Update the database after selling a symbol.

    :param api_num: The API number (1 or 2).
    :param symbol: The symbol that was sold.
    :param amount_sold: The amount of the symbol that was sold.
    """
    # Fetch the current USDT balance and update symbol holdings
    asset_usdt_balance = account.get_api_balance(api_num)

    # Connect to the SQLite database
    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        # Query the latest row of data
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()

        # Retrieve current balances and holdings
        current_usdt_balance = row[f"USDT_API_{api_num}"]
        current_asset_value = row[f"ASSET_API_{api_num}"]
        symbol_holdings_str = row[f"symbol_API_{api_num}"]
        current_symbol_holdings = (
            json.loads(symbol_holdings_str) if symbol_holdings_str else {}
        )

        # Calculate the value received from the sale and update USDT balance
        coin_asset = amount_sold * current_price
        current_usdt_balance += coin_asset

        # Update the holdings after sale
        if current_symbol_holdings.get(symbol, 0) >= amount_sold:
            current_symbol_holdings[symbol] -= amount_sold
        else:
            # Handle error or log if sold amount is greater than holdings
            print("Error: Selling more than holdings.")

        total_coin_value = 0  # Total value of all coins
        for coin, quantity in current_symbol_holdings.items():
            coin_value = quantity * account.get_current_price(coin)
            total_coin_value += coin_value

        current_asset_value = (
            total_coin_value + current_usdt_balance
        )  # Update total asset value

        # Update the database with new balances and holdings
        cursor.execute(
            f"""
            UPDATE trades
            SET USDT_API_{api_num} = ?, ASSET_API_{api_num} = ?, symbol_API_{api_num} = ?
            WHERE id = ?
            """,
            (
                current_usdt_balance,
                current_asset_value,
                json.dumps(current_symbol_holdings),
                row["id"],
            ),
        )
        border = "+" + "-" * 45 + "SELL" + "-" * 45 + "+"
        print(border)
        print(f"USDT_API_{api_num} updated to: {current_usdt_balance}")
        print(f"ASSET_API_{api_num} updated to: {current_asset_value}")
        print(f"symbol_API_{api_num} updated to: {json.dumps(current_symbol_holdings)}")

        conn.commit()


def update_without_trade(api_num, account):
    """
    Update the database without any trade (buy or sell), maintaining the current asset values.

    :param api_num: The API number (1 or 2).
    """
    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Retrieve the latest row data
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()

        # Update the latest row with the current time
        cursor.execute(
            f"""
            UPDATE trades
            SET date = ?, USDT_API_{api_num} = ?, symbol_API_{api_num} = ?, ROI_API_{api_num} = ?, ASSET_API_{api_num} = ?
            WHERE id = ?
            """,
            (
                current_time,
                row[f"USDT_API_{api_num}"],
                row[f"symbol_API_{api_num}"],
                row[f"ROI_API_{api_num}"],
                row[f"ASSET_API_{api_num}"],
                row["id"],
            ),
        )

        conn.commit()
        print(f"Data updated without trade for API {api_num} at {current_time}.")


def update_total_assets_and_roi(api_num, account):
    """
    Update the database without any trade (buy or sell), using the latest data for total assets and ROI.

    :param api_num: The API number (1 or 2).
    :param account: The BinanceAccount instance.
    """
    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 마지막 행의 데이터를 가져옵니다
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()

        # 마지막에서 두 번째 행의 데이터를 가져옵니다
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1 OFFSET 1")
        second_last_row = cursor.fetchone()

        # 마지막 행의 USDT 및 코인 보유 정보를 가져옵니다
        usdt_balance = row[f"USDT_API_{api_num}"]
        symbol_holdings_str = row[f"symbol_API_{api_num}"]
        current_symbol_holdings = (
            json.loads(symbol_holdings_str) if symbol_holdings_str else {}
        )

        # 총 자산을 계산합니다
        total_assets = usdt_balance
        for symbol, quantity in current_symbol_holdings.items():
            current_price = account.get_current_price(symbol)
            total_assets += quantity * current_price

        # ROI 계산 (마지막에서 두 번째 행의 자산 대비 현재 총 자산의 변화율)
        initial_asset = second_last_row[f"ASSET_API_{api_num}"]
        roi = (
            ((total_assets - initial_asset) / initial_asset) * 100
            if initial_asset != 0
            else 0
        )

        # 새로운 업데이트를 위한 현재 시간
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 데이터베이스에 새로운 행을 추가합니다
        cursor.execute(
            f"""
            UPDATE trades
            SET date = ?, ASSET_API_{api_num} = ?, ROI_API_{api_num} = ?
            WHERE id = ?
            """,
            (current_time, total_assets, roi, row["id"]),
        )

        conn.commit()
        print(f"Data updated without trade for API {api_num} at {current_time}.")
        border = "+" + "-" * 90 + "+"
        print(border)
        print("|" + f"API_{api_num} 최종 자산: {total_assets} ROI: {roi}" + "|")
        print(border)


def update_final_assets():
    """
    모든 태스크가 완료된 후 최종 자산과 ROI를 데이터베이스에서 가져와 업데이트합니다.
    """
    with sqlite3.connect(
        "/home/hanvit4303/dk_tradebot/server/spot_stockdata_fastapi.db"
    ) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 최종 업데이트를 위한 현재 시간
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 마지막 행의 데이터를 조회
        cursor.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()

        if row:
            # API 1과 API 2의 자산을 합산하여 총 자산 계산
            total_asset = row["ASSET_API_1"] + row["ASSET_API_2"]

            # 마지막 행의 총 자산(ASSET) 업데이트
            cursor.execute(
                """
                UPDATE trades
                SET ASSET = ?
                WHERE id = ?
                """,
                (total_asset, row["id"]),
            )

            conn.commit()

            print("|" + " " * 90 + "|")
            print(
                "|" + "FINAL ASSETS {} completed".format(total_asset).center(90) + "|"
            )
            print("|" + " " * 90 + "|")
            border = "+" + "-" * 90 + "+"
            print(border)
