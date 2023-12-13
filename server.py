from fastapi import FastAPI

app = FastAPI(
    title="DK Stock Portfolio",
    description="This is a spot trading bot",
)


@app.get("/")
def aa():
    return "보낼 값"


from fastapi import FastAPI
import sqlite3
from typing import List

app = FastAPI()


# 데이터베이스에서 모든 trades 레코드를 가져오는 함수
def get_trades():
    conn = sqlite3.connect("/home/hanvit4303/dk_tradebot/spot_stockdata.db")
    conn.row_factory = (
        sqlite3.Row
    )  # This enables column access by name: row['column_name']
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trades")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


# 모든 trades 데이터를 JSON 형태로 반환하는 엔드포인트
@app.get("/spottrades/", response_model=List[dict])
async def read_trades():
    return get_trades()
