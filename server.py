from fastapi import FastAPI, HTTPException
import sqlite3
from typing import List
import logging
import requests
import pandas as pd


# Create a single instance of FastAPI
app = FastAPI(
    title="DK Stock Portfolio",
    description="This is a spot trading bot",
)


# Define a root endpoint
@app.get("/")
def root():
    response = requests.get("https://api.binance.com/api/v3/ticker/24hr")
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    return df


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
