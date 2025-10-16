import clickhouse_connect
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

from utils import fetch_prev_epoch_info

def load_env(dotenv_path):
    load_dotenv(dotenv_path)
    return {
        "old_host": os.getenv("CLICKHOUSE_HOST"),
        "old_port": int(os.getenv("CLICKHOUSE_PORT")),
        "old_username": os.getenv("CLICKHOUSE_USERNAME"),
        "old_password": os.getenv("CLICKHOUSE_PASSWORD"),
        "host": os.getenv("NEW_CLICKHOUSE_HOST"),
        "port": int(os.getenv("NEW_CLICKHOUSE_PORT")),
        "username": os.getenv("NEW_CLICKHOUSE_USERNAME"),
        "password": os.getenv("NEW_CLICKHOUSE_PASSWORD")
    }

# Load credentials from .env
config = load_env(dotenv_path=".env")

# Initialize ClickHouse client
# Binjiang's old server DB
old_client = clickhouse_connect.get_client(
    host=config["old_host"],
    port=config["old_port"],
    username=config["old_username"],
    password=config["old_password"]
)
# New server DB
client = clickhouse_connect.get_client(
    host=config["host"],
    port=config["port"],    
    username=config["username"],
    password=config["password"]
)

def query_sandwich_txs(): 
    query = f"""
    SELECT *
    FROM solwich.sandwiches_nobundle
    WHERE slot >= 37000000
    ORDER BY slot DESC
    LIMIT 50
    """
    res = old_client.query(query)
    df = pd.DataFrame(res.result_rows, columns=res.column_names)
    return df

if __name__ == "__main__":
    prev_epoch, prev_epoch_start, prev_epoch_end = fetch_prev_epoch_info()
    print(f"Previous epoch: {prev_epoch}, Start slot: {prev_epoch_start}, End slot: {prev_epoch_end}")
    
    print("Fetching recent sandwich transactions...")
    df = query_sandwich_txs()
    print(df)