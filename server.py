from fastapi import FastAPI, HTTPException
import sqlite3
from typing import List
import logging
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Create a single instance of FastAPI
app = FastAPI(
    title="DK Stock Portfolio",
    description="This is a spot trading bot",
)

session = requests.Session()

# 재시도 횟수, 재시도 간의 백오프 팩터(지수적으로 증가하는 대기 시간), 상태 코드에 대한 재시도를 설정합니다.
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])

# HTTP 어댑터에 재시도 설정을 추가합니다.
session.mount("https://", HTTPAdapter(max_retries=retries))


# Define a root endpoint
@app.get("/")
def root():
    try:
        # 세션을 사용하여 요청을 합니다.
        response = session.get("https://api.binance.com/api/v3/ticker/24hr")
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
        # 데이터 처리 로직...
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        error_message = f"HTTP 오류 발생: {http_err}"
        return error_message
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        error_message = f"연결 오류 발생: {conn_err}"
        return error_message
    except Exception as err:
        # 기타 오류 출력
        error_message = f"기타 오류 발생: {err}"
        return error_message


@app.get("/ha")
def root2():
    return {"message": "W222"}


logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Function to get trades from the database
def get_trades():
    conn = sqlite3.connect("./spot_stockdata_fastapi.db")
    conn.row_factory = sqlite3.Row  # Enables column access by name: row['column_name']
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM trades")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/spottrades/", response_model=List[dict])
async def read_trades():
    try:
        return get_trades()
    except Exception as e:
        # Log the exception
        logging.exception("An error occurred while fetching trades.")
        # Respond with an HTTP 500 error
        raise HTTPException(status_code=500, detail=str(e))
