from fastapi import FastAPI, HTTPException, Query
import sqlite3
from typing import List
import logging
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time  # time 모듈 추가

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
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 10 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

        params = {
            "symbol": "BTCBUSD",
            "interval": "1h",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = session.get("https://api.binance.com/api/v3/klines", params=params)
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


@app.get("/fetchdata")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 10 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

        params = {
            "symbol": symbol,
            "interval": "1h",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch1m")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - 1 * 6 * 60 * 60 * 1000

        params = {
            "symbol": symbol,
            "interval": "1m",  # 5분 간격으로 변경
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()
        data = response.json()

        # 10분 간격의 데이터로 병합하는 로직을 여기에 추가합니다.
        # 예: 인접한 두 캔들스틱의 데이터를 병합하여 하나의 10분 캔들스틱을 생성

        return data  # 병합된 10분 간격 데이터 반환
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch3m")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - 1 * 12 * 60 * 60 * 1000  # 예: 7일 전 시간

        params = {
            "symbol": symbol,
            "interval": "3m",  # 5분 간격으로 변경
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()
        data = response.json()

        # 10분 간격의 데이터로 병합하는 로직을 여기에 추가합니다.
        # 예: 인접한 두 캔들스틱의 데이터를 병합하여 하나의 10분 캔들스틱을 생성

        return data  # 병합된 10분 간격 데이터 반환
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch5m")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)
        start_time = end_time - 1 * 24 * 60 * 60 * 1000  # 예: 7일 전 시간

        params = {
            "symbol": symbol,
            "interval": "5m",  # 5분 간격으로 변경
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()
        data = response.json()

        # 10분 간격의 데이터로 병합하는 로직을 여기에 추가합니다.
        # 예: 인접한 두 캔들스틱의 데이터를 병합하여 하나의 10분 캔들스틱을 생성

        return data  # 병합된 10분 간격 데이터 반환
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch15m")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  #
        start_time = end_time - 3 * 24 * 60 * 60 * 1000  #

        params = {
            "symbol": symbol,
            "interval": "15m",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch30m")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 7 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

        params = {
            "symbol": symbol,
            "interval": "30m",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch4h")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 20 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

        params = {
            "symbol": symbol,
            "interval": "4h",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch2h")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 30 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

        params = {
            "symbol": symbol,
            "interval": "2h",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/fetch1day")
async def fetch_data(symbol: str = Query(...)):
    try:
        end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
        start_time = end_time - 240 * 24 * 60 * 60 * 1000  # 30일 전 시간을 밀리초로 계산

        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": start_time,
            "endTime": end_time,
        }
        # 세션을 사용하여 요청을 합니다.
        response = requests.get("https://api.binance.com/api/v3/klines", params=params)
        response.raise_for_status()  # 실패한 상태 코드에 대해 예외를 발생시킵니다.
        data = response.json()  # 성공 시, JSON 데이터를 파싱합니다.
        return data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


@app.get("/ha")
def root2():
    return {"message": "W222"}


logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_historical_data(symbol):
    end_time = int(time.time() * 1000)  # 현재 시간을 밀리초로 계산
    start_time = end_time - 10 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

    params = {
        "symbol": symbol,
        "interval": "1h",
        "startTime": start_time,
        "endTime": end_time,
    }

    response = session.get("https://api.binance.com/api/v3/klines", params=params)
    response.raise_for_status()
    data = response.json()
    return data


@app.get("/historical_data/")
async def read_historical_data():
    trades = get_trades()
    historical_data = {}
    for trade in trades:
        symbol_api_1 = trade["symbol_API_1"]
        symbol_api_2 = trade["symbol_API_2"]
        historical_data[symbol_api_1] = get_historical_data(symbol_api_1)
        historical_data[symbol_api_2] = get_historical_data(symbol_api_2)
    return historical_data


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
