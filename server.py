import json
from fastapi import Body, FastAPI, HTTPException, Query
import sqlite3
from typing import List
import logging
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time  # time 모듈 추가
import os

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


# BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
# BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# # 바이낸스 클라이언트 인스턴스 생성
# client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
# @app.get("/futures_account_balance")
# async def get_futures_account_balance():
#     try:
#         # 바이낸스 API를 호출하여 선물 계좌의 잔고 정보를 가져옵니다.
#         account_balance = client.futures_account_balance()
#         return account_balance
#     except HTTPError as http_err:
#         raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
#     except Exception as err:
#         raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


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
        start_time = end_time - 30 * 24 * 60 * 60 * 1000  # 10일 전 시간을 밀리초로 계산

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
        # 원래 240
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


@app.get("/fetch_history")
async def fetch_history(symbol: str = Query(...), interval: str = Query("30m")):
    try:
        limit = 500  # 최대 데이터 포인트 수 (바이낸스 제한)
        total_data_points = 3000  # 원하는 데이터 포인트 수
        all_data = []  # 데이터를 저장할 리스트

        # 현재 시간을 밀리초로 계산
        end_time = int(time.time() * 1000)

        # 반복하여 데이터 가져오기
        for _ in range(total_data_points // limit):
            # API 파라미터 설정
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "endTime": end_time,
            }

            # 바이낸스 API 호출
            response = requests.get(
                "https://api.binance.com/api/v3/klines", params=params
            )
            response.raise_for_status()
            data = response.json()

            # 데이터 추가
            all_data.extend(data)

            # 다음 호출을 위한 종료 시간 업데이트
            end_time = data[0][0]  # 첫 번째 데이터의 'Open Time'

        return all_data
    except requests.exceptions.HTTPError as http_err:
        # HTTP 오류 출력
        raise HTTPException(status_code=400, detail=f"HTTP 오류 발생: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        # 연결 오류 출력
        raise HTTPException(status_code=400, detail=f"연결 오류 발생: {conn_err}")
    except Exception as err:
        # 기타 오류 출력
        raise HTTPException(status_code=500, detail=f"기타 오류 발생: {err}")


# 이 함수는 FastAPI 경로 '/fetch_data'에 설정되어 있으며, 심볼과 간격을 입력으로 받아 해당 데이터를 JSON 형식으로 반환합니다.
@app.post("/update_grid_account")
async def update_grid_account(data: dict = Body(...)):
    try:
        # 파일이 존재하는지 확인하고, 존재하면 데이터를 불러옵니다.
        filename = "grid_account_updated.json"
        if os.path.exists(filename):
            with open(filename, "r") as file:
                existing_data = json.load(file)
        else:
            existing_data = {}

        latest_key = max(existing_data.keys(), default=None)

        # if "historyInfo" not in data and "historyInfo" in existing_data.get(
        #     latest_key, {}
        # ):
        #     data["historyInfo"] = existing_data[latest_key]["historyInfo"]
        # if "historyInfo" in data and "historyInfo" in existing_data.get(latest_key, {}):
        #     data["historyInfo"].update(existing_data[latest_key]["historyInfo"])

        # 새 데이터 추가
        existing_data.update(data)

        # 키 개수가 3000개를 초과하는 경우, 가장 오래된 데이터를 삭제
        while len(existing_data) > 3000:
            oldest_key = min(existing_data.keys())
            del existing_data[oldest_key]

        # 수정된 데이터를 파일에 저장
        with open(filename, "w") as file:
            json.dump(existing_data, file, indent=4)

        return {"message": "Data updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


# 이 함수는 FastAPI 경로 '/fetch_data'에 설정되어 있으며, 심볼과 간격을 입력으로 받아 해당 데이터를 JSON 형식으로 반환합니다.
@app.post("/historyaccount")
async def update_grid_account(data: dict = Body(...)):
    try:
        # 파일이 존재하는지 확인하고, 존재하면 데이터를 불러옵니다.
        filename = "historyaccount.json"
        if os.path.exists(filename):
            with open(filename, "r") as file:
                existing_data = json.load(file)
        else:
            existing_data = {}

        existing_data.update(data)

        # 키 개수가 3000개를 초과하는 경우, 가장 오래된 데이터를 삭제
        while len(existing_data) > 3000:
            oldest_key = min(existing_data.keys())
            del existing_data[oldest_key]

        # 수정된 데이터를 파일에 저장
        with open(filename, "w") as file:
            json.dump(existing_data, file, indent=4)

        return {"message": "Data updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/get_grid_account")
async def get_grid_account_updated():
    """
    Returns the data from grid_account_updated.json.
    """
    try:
        # 파일에서 데이터를 읽어옵니다.
        with open("grid_account_updated.json", "r") as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/gethistoryaccount")
async def get_grid_account_updated():
    """
    Returns the data from grid_account_updated.json.
    """
    try:
        # 파일에서 데이터를 읽어옵니다.
        with open("historyaccount.json", "r") as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
