import clickhouse_connect
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

from utils import get_prev_epoch_info

def load_env():
    load_dotenv(dotenv_path=".old.env")
    return {
        "host": os.getenv("CLICKHOUSE_HOST"),
        "port": int(os.getenv("CLICKHOUSE_PORT")),
        "username": os.getenv("CLICKHOUSE_USERNAME"),
        "password": os.getenv("CLICKHOUSE_PASSWORD")
    }

# Load credentials from .env
config = load_env()

# Initialize ClickHouse client
client = clickhouse_connect.get_client(
    host='209.192.245.26',
    port=8123,
    username='sol',
    password='sol'
)

def query_sandwich_txs(): 
    query = f"""
    SELECT *
    FROM solwich.sandwiches_nobundle
    WHERE slot >= 37000000
    ORDER BY slot DESC
    LIMIT 50
    """
    res = client.query(query)
    df = pd.DataFrame(res.result_rows, columns=res.column_names)
    return df

if __name__ == "__main__":
    prev_epoch, prev_epoch_start, prev_epoch_end = get_prev_epoch_info()
    print(f"Previous epoch: {prev_epoch}, Start slot: {prev_epoch_start}, End slot: {prev_epoch_end}")
    
    print("Fetching recent sandwich transactions...")
    df = query_sandwich_txs()
    print(df)