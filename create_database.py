import sqlite3
import json

# SQLite 데이터베이스 파일 생성 및 연결
conn = sqlite3.connect("./server/spot_stockdata_fastapi.db")

# 커서 객체 생성
cursor = conn.cursor()
# JSON으로 변환

# 테이블 생성
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY,
        name TEXT,
        date TEXT,
        USDT_API_1 REAL,
        symbol_API_1 TEXT,
        ROI_API_1 REAL,
        ASSET_API_1 REAL,
        USDT_API_2 REAL,
        symbol_API_2 TEXT,
        ROI_API_2 REAL,
        ASSET_API_2 REAL,
        ASSET REAL
    )
"""
)
conn.commit()

# 연결 종료
conn.close()
