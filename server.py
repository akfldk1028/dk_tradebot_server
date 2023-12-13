from fastapi import FastAPI, HTTPException
import sqlite3
from typing import List

# Create a single instance of FastAPI
app = FastAPI(
    title="DK Stock Portfolio",
    description="This is a spot trading bot",
)


# Define a root endpoint
@app.get("/")
def root():
    return {"message": "Welcome to the DK Stock Portfolio"}


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
        raise HTTPException(status_code=500, detail=str(e))
