import clickhouse_connect
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

def load_env():
    load_dotenv()
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

def get_prev_epoch_info():
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
    print(f"Current epoch: {data['epoch']}, Slot: {data['absoluteSlot']}, Slot index in epoch: {data['slotIndex']}, Slots in epoch: {data['slotsInEpoch']}")

    current_epoch = data["epoch"]
    current_slot = data["absoluteSlot"]
    slot_index = data["slotIndex"]
    slots_in_epoch = data["slotsInEpoch"]

    current_epoch_start = current_slot - slot_index
    prev_epoch = current_epoch - 1
    prev_epoch_start = current_epoch_start - slots_in_epoch
    prev_epoch_end = current_epoch_start - 1

    return prev_epoch, prev_epoch_start, prev_epoch_end

def get_validator_info_from_clickhouse(epoch: int) -> pd.DataFrame:
    """
    Query validator metadata (commission, stake, etc.) for the given epoch from ClickHouse.
    """
    query = f"""
    SELECT
        node_pubkey AS validator_account,
        vote_pubkey AS vote_account,
        commission / 100.0 AS commission,
        activated_stake / 1e9 AS total_stake
    FROM solwich.epoch_vote_accounts
    WHERE epoch = {epoch}
    """
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)

def query_victim_counts(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count backrun attacks per validator in the given slot range.
    """
    query = f'''
    SELECT l.leader, COUNT(*) AS victim_count
    FROM solwich.sandwiches_nobundle AS s
    ANY INNER JOIN solwich.slot_leaders AS l ON s.slot = l.slot
    WHERE s.slot BETWEEN {start_slot} AND {end_slot}
      AND s.type = 'backrun'
    GROUP BY l.leader
    ORDER BY victim_count DESC
    '''
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    df.rename(columns={"leader": "validator_account"}, inplace=True)
    return df

def query_slot_counts(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count total number of slots led by each validator in the given range.
    """
    query = f'''
    SELECT leader AS validator_account, COUNT(*) AS slot_sum
    FROM solwich.slot_leaders
    WHERE slot BETWEEN {start_slot} AND {end_slot}
    GROUP BY leader
    '''
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)

def query_total_victim_counts(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count total number of victims in all slots led by each validator in the given range.
    """
    query = f'''
    SELECT
        l.leader AS validator_account,
        SUM(v.victim_count) AS total_victim_count
    FROM (
        SELECT slot, COUNT(*) AS victim_count
        FROM solwich.sandwiches_nobundle
        WHERE type = 'backrun'
          AND slot BETWEEN {start_slot} AND {end_slot}
        GROUP BY slot
    ) AS v
    INNER JOIN solwich.slot_leaders AS l ON v.slot = l.slot
    GROUP BY l.leader
    '''
    result = client.query(query)
    return pd.DataFrame(result.result_rows, columns=result.column_names)

if __name__ == "__main__":
    # Step 1: Fetch previous epoch metadata
    prev_epoch, start_slot, end_slot = get_prev_epoch_info()
    print(f"Previous epoch: {prev_epoch}, Slot range: {start_slot} to {end_slot}")

    # Step 2: Query validator stake and commission info
    validators_df = get_validator_info_from_clickhouse(prev_epoch)

    # Step 3: Query victim counts (how many backruns in each validator's slots)
    victim_df = query_victim_counts(start_slot, end_slot)

    # Step 4: Merge and compute victim stats
    final_df = pd.merge(validators_df, victim_df, on="validator_account", how="left")
    final_df["victim_count"] = final_df["victim_count"].fillna(0).astype(int)
    final_df["commission"] = final_df["commission"].round(2)
    final_df["total_stake"] = final_df["total_stake"].round(2)

    # Compute weighted victim count (stake-weighted)
    total_stake = final_df["total_stake"].sum()
    final_df["weighted_victim_count"] = ((final_df["total_stake"] / total_stake) * final_df["victim_count"]).round(4)

    # Step 5: Query slot leadership counts
    slot_sum_df = query_slot_counts(start_slot, end_slot)
    final_df = pd.merge(final_df, slot_sum_df, on="validator_account", how="left")
    final_df["slot_sum"] = final_df["slot_sum"].fillna(0).astype(int)

    # Compute victim slot ratio (victim slots / total slots led)
    final_df["victim_slot_ratio"] = final_df.apply(
        lambda row: round(row["victim_count"] / row["slot_sum"], 4) if row["slot_sum"] > 0 else 0,
        axis=1
    )

    # Step 6: Query total number of victims across slots
    total_victim_df = query_total_victim_counts(start_slot, end_slot)
    final_df = pd.merge(final_df, total_victim_df, on="validator_account", how="left")
    final_df["total_victim_count"] = final_df["total_victim_count"].fillna(0).astype(int)

    # Compute average number of victims per slot
    final_df["avg_victim_count"] = final_df.apply(
        lambda row: round(row["total_victim_count"] / row["slot_sum"], 2) if row["slot_sum"] > 0 else 0,
        axis=1
    )

    # Add epoch column
    final_df["epoch_num"] = prev_epoch

    # Reorder columns for output
    final_df = final_df[[
        "epoch_num", "validator_account", "vote_account", "commission", "total_stake",
        "victim_count", "total_victim_count", "avg_victim_count",
        "weighted_victim_count", "slot_sum", "victim_slot_ratio"
    ]].sort_values(by="victim_count", ascending=False)

    # Export to Excel
    timestamp = datetime.now().strftime("%Y_%m_%d_%H-%M")
    filename = f"epoch_validator_stats_inbundle_all_{timestamp}.csv"
    final_df.to_csv(filename, index=False)
    print(f"CSV file saved: {filename}")

    # Optional: insert into ClickHouse
    # client.insert_df('solwich.epoch_validator_stats_inbundle_all', final_df)
