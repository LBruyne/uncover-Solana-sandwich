import clickhouse_connect
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

def load_env():
    load_dotenv(dotenv_path="../watcher/.env")
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
    host=config["host"],
    port=config["port"],
    username=config["username"],
    password=config["password"]
)

solana_rpc_url = "https://api.mainnet-beta.solana.com"


def fetch_prev_epoch_info():
    """
    Fetch Solana epoch information via JSON-RPC and compute the previous epoch's ID and slot range.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo"
    }
    response = requests.post(solana_rpc_url, json=payload)
    data = response.json()["result"]
    print(f"Current epoch: {data['epoch']}, Current Slot: {data['absoluteSlot']}, Start slot: {data['absoluteSlot'] - data['slotIndex']}, End slot: {data['absoluteSlot'] + (data['slotsInEpoch'] - data['slotIndex'] - 1)}, Slot index in epoch: {data['slotIndex']}, Total slots in epoch: {data['slotsInEpoch']}")

    current_epoch = data["epoch"]
    current_slot = data["absoluteSlot"]
    slot_index = data["slotIndex"]
    slots_in_epoch = data["slotsInEpoch"]

    current_epoch_start = current_slot - slot_index
    prev_epoch = current_epoch - 1
    prev_epoch_start = current_epoch_start - slots_in_epoch
    prev_epoch_end = current_epoch_start - 1

    return prev_epoch, prev_epoch_start, prev_epoch_end

def query_current_slots_in_DB(start_slot=0, end_slot=500000000):
    """
    Query the ClickHouse DB to find the min and max slot stored in the slot_leaders table.
    """
    query = f"""
    SELECT
        min(slot) AS min_slot,
        max(slot) AS max_slot,
        countDistinct(slot) AS total_slots
    FROM solwich.slot_txs
    WHERE slot BETWEEN {start_slot} AND {end_slot} AND txFetched = true
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    min_slot = df['min_slot'].iloc[0]
    max_slot = df['max_slot'].iloc[0]
    total_slots = df['total_slots'].iloc[0]
    return min_slot, max_slot, total_slots


def query_leader_slot_counts(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count TOTAL number of slots led by each leader in [start_slot, end_slot],
    irrespective of any sandwich filters.
    """
    
    # Total slots led by each validator in the given slot range in schedule
    query = f"""
    SELECT
        leader,
        count() AS slot_count
    FROM solwich.slot_leaders
    WHERE slot BETWEEN {start_slot} AND {end_slot}
    GROUP BY leader
    ORDER BY slot_count DESC
    """
    res_schedule = client.query(query)
    df_schedule = pd.DataFrame(res_schedule.result_rows, columns=res_schedule.column_names)
    
    # Total slots led by each validator in the given slot range in actual
    query = f"""
    SELECT
        l.leader,
        countDistinct(s.slot) AS actual_slot_count
    FROM solwich.slot_leaders AS l
    LEFT JOIN solwich.slot_txs  AS s
        ON s.slot = l.slot
    WHERE l.slot BETWEEN {start_slot} AND {end_slot} AND txFetched = true
    GROUP BY l.leader
    """
    res_actual = client.query(query)
    df_actual = pd.DataFrame(res_actual.result_rows, columns=res_actual.column_names)

    out = df_schedule.merge(df_actual, on="leader", how="left")
    out["actual_slot_count"] = out["actual_slot_count"].fillna(0).astype(int)
    out["slot_count"] = out["slot_count"].astype(int)
    out = out.rename(columns={"leader": "validator_account"}) \
             .sort_values(["actual_slot_count", "slot_count"], ascending=[False, False]) \
             .reset_index(drop=True)
    return out

def query_sandwiches_by_slot(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count sandwiches data per validator in the given slot range.
    """
    query = f'''
    SELECT
        l.leader,
        countDistinct(s.sandwichId) AS sandwich_count,               
        sum(s.victimCount) AS total_victim_count,      
        sumIf(s.profitA, s.tokenA = 'SOL') AS total_SOL_profit
    FROM solwich.sandwiches AS s
    INNER JOIN solwich.slot_leaders AS l ON s.slot = l.slot
    WHERE s.slot BETWEEN {start_slot} AND {end_slot} AND s.tokenA = 'SOL' AND s.ownerSame = true AND s.crossBlock = false
    GROUP BY l.leader
    ORDER BY total_victim_count DESC
    '''
    result = client.query(query)
    res = pd.DataFrame(result.result_rows, columns=result.column_names)
    res.rename(columns={"leader": "validator_account"}, inplace=True)
    return res


if __name__ == "__main__":
    min_slot, max_slot, total_slots = query_current_slots_in_DB()
    print(f"Current checked slots in DB: min_slot = {min_slot}, max_slot = {max_slot}, total_slots = {total_slots}")
    
    # Step 1: Fetch last epoch info
    prev_epoch, prev_epoch_start, prev_epoch_end = fetch_prev_epoch_info()
    print(f"Previous epoch: {prev_epoch}, Start slot: {prev_epoch_start}, End slot: {prev_epoch_end}")
    
    min_slot, max_slot, total_slots = query_current_slots_in_DB(prev_epoch_start, prev_epoch_end)
    print(f"Current checked slots in DB in previous epoch range: min_slot = {min_slot}, max_slot = {max_slot}, total_slots = {total_slots}")
    
    # Step 2: Query victim, slot counts in the previous epoch
    sandwiches = query_sandwiches_by_slot(prev_epoch_start, prev_epoch_end)
    print(f"Victim counts in epoch {prev_epoch} (slots {prev_epoch_start} to {prev_epoch_end}):")
    print(sandwiches.head(10))
    print("Total sandwiches:", sandwiches['sandwich_count'].sum())
    print("Total profit in SOL:", sandwiches['total_SOL_profit'].sum())
    print("Total victims:", sandwiches['total_victim_count'].sum())
        
    slots = query_leader_slot_counts(prev_epoch_start, prev_epoch_end)
    print(f"Total slots led in epoch {prev_epoch} (slots {prev_epoch_start} to {prev_epoch_end}):")
    print(slots.head(10))
    print("Total slots:", slots['slot_count'].sum(), "Total actual slots checked:", slots['actual_slot_count'].sum())

    # Compute slot ratio (victims / total slots led)
    slots['victim_slot_ratio'] = sandwiches['total_victim_count'] / slots['actual_slot_count']
    slots['sandwich_slot_ratio'] = sandwiches['sandwich_count'] / slots['actual_slot_count']
    # Compute weighted victim count (slot led / total slots * victims)
    slots['weighted_victim_count'] = (slots['actual_slot_count'] / slots['actual_slot_count'].sum()) * sandwiches['total_victim_count']
    slots['weighted_sandwich_count'] = (slots['actual_slot_count'] / slots['actual_slot_count'].sum()) * sandwiches['sandwich_count']
    slots = slots.fillna(0)
    slots = slots.sort_values(by='sandwich_slot_ratio', ascending=False).reset_index(drop=True)
    # Merge
    slots = slots.merge(sandwiches[['validator_account', 'sandwich_count', 'total_victim_count', 'total_SOL_profit']], on="validator_account", how="left")
    print(f"In epoch {prev_epoch} (slots {prev_epoch_start} to {prev_epoch_end}):")
    print(slots.head(10))

    # Export to Excel
    timestamp = datetime.now().strftime("%Y_%m_%d_%H-%M")
    filename = f"epoch_validator_stats_new_{timestamp}.csv"
    slots.to_csv(filename, index=False)
    print(f"CSV file saved: {filename}")