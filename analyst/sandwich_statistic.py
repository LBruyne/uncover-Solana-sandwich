import clickhouse_connect
import pandas as pd
import numpy as np
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import itertools

from utils import fetch_prev_epoch_info

START_SLOT = 370453692


def load_env():
    load_dotenv(dotenv_path=".env")
    return {
        "host": os.getenv("NEW_CLICKHOUSE_HOST"),
        "port": int(os.getenv("NEW_CLICKHOUSE_PORT")),
        "username": os.getenv("NEW_CLICKHOUSE_USERNAME"),
        "password": os.getenv("NEW_CLICKHOUSE_PASSWORD"),
    }


# Load credentials from .env
config = load_env()
# Initialize ClickHouse client
client = clickhouse_connect.get_client(
    host=config["host"],
    port=config["port"],
    username=config["username"],
    password=config["password"],
)


def query_current_slots_in_DB(start_slot=0, end_slot=500000000):
    """
    Query the ClickHouse DB to find the min and max slot stored
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
    min_slot = df["min_slot"].iloc[0]
    max_slot = df["max_slot"].iloc[0]
    total_slots = df["total_slots"].iloc[0]
    return min_slot, max_slot, total_slots


def query_leader_slot_counts(start_slot: int, end_slot: int) -> pd.DataFrame:
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
    df_schedule = pd.DataFrame(
        res_schedule.result_rows, columns=res_schedule.column_names
    )

    # Total slots actually led by each validator in the given slot range, checked in DB
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
    out = (
        out.rename(columns={"leader": "validator"})
        .sort_values(["actual_slot_count", "slot_count"], ascending=[False, False])
        .reset_index(drop=True)
    )
    return out


def query_sandwiches(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count sandwiches data in the given slot range.
    """
    query = f"""
    SELECT *
    FROM solwich.sandwiches
    WHERE slot BETWEEN {start_slot} AND {end_slot}
    ORDER BY slot DESC, sandwichId DESC
    LIMIT 1 BY sandwichId
    """
    result = client.query(query)
    res = pd.DataFrame(result.result_rows, columns=result.column_names)
    return res


if __name__ == "__main__":
    min_slot, max_slot, total_slots = query_current_slots_in_DB(start_slot=START_SLOT)
    print(
        f"Current fetched slots in DB: min = {min_slot}, max = {max_slot}, total = {total_slots}"
    )

    prev_epoch, prev_epoch_start, prev_epoch_end = fetch_prev_epoch_info()
    print(
        f"Previous epoch: {prev_epoch}, start: {prev_epoch_start}, end: {prev_epoch_end}"
    )

    min_slot, max_slot, total_slots = query_current_slots_in_DB(
        prev_epoch_start, prev_epoch_end
    )
    print(
        f"Current fetched slots in DB in previous epoch range: min = {min_slot}, max = {max_slot}, total = {total_slots}"
    )

    # Query total victims number, sandwich number and SOL profit per validator in the previous epoch
    sandwiches = query_sandwiches(prev_epoch_start, prev_epoch_end)
    in_block_sandwiches = sandwiches[sandwiches["crossBlock"] == False]
    cross_block_sandwiches = sandwiches[sandwiches["crossBlock"] == True]
    print(
        f"Previous epoch {prev_epoch} sandwiches summary (slot {prev_epoch_start} to {prev_epoch_end}):"
    )
    print(
        f"Total sandwiches: {len(sandwiches)}, in-block: {len(in_block_sandwiches)}, cross-block: {len(cross_block_sandwiches)}"
    )

    ib = in_block_sandwiches.copy()
    cb = cross_block_sandwiches.copy()

    # Normalize types for safety
    for df_ in (ib, cb):
        df_["victimCount"] = (
            pd.to_numeric(df_["victimCount"], errors="coerce").fillna(0).astype(int)
        )
        # profitA -> float
        df_["profitA"] = pd.to_numeric(df_["profitA"], errors="coerce").fillna(0.0)
        # tokenA presence
        # profitA in SOL only
        df_["profitA_SOL"] = df_["profitA"].where(df_["tokenA"] == "SOL", 0.0)

    label_map = {
        (True, True): "SSSO",
        (True, False): "SSDO",
        (False, True): "DSSO",
        (False, False): "DSDO",
    }
    order = ["SSSO", "SSDO", "DSSO", "DSDO"]

    def _aggregate_three_metrics(df_):
        # count sandwiches per category
        cnt = (
            df_.groupby(["signerSame", "ownerSame"])
            .size()
            .reset_index(name="sandwiches")
        )
        # victims & profitA_SOL per category
        sums = df_.groupby(["signerSame", "ownerSame"], as_index=False).agg(
            victims=("victimCount", "sum"), profitA_SOL=("profitA_SOL", "sum")
        )
        # ensure all four combos exist
        combos = pd.DataFrame(
            list(itertools.product([False, True], [False, True])),
            columns=["signerSame", "ownerSame"],
        )
        out = (
            combos.merge(cnt, on=["signerSame", "ownerSame"], how="left")
            .merge(sums, on=["signerSame", "ownerSame"], how="left")
            .fillna({"sandwiches": 0, "victims": 0, "profitA_SOL": 0.0})
        )
        out["sandwiches"] = out["sandwiches"].astype(int)
        out["victims"] = out["victims"].astype(int)
        out["profitA_SOL"] = pd.to_numeric(out["profitA_SOL"], errors="coerce").fillna(
            0.0
        )
        out["category"] = out.apply(
            lambda r: label_map[(r["signerSame"], r["ownerSame"])], axis=1
        )
        out = out.set_index("category").loc[order].reset_index()
        return out[["category", "sandwiches", "victims", "profitA_SOL"]]

    # -------- In-block figure --------
    agg_ib = _aggregate_three_metrics(ib)
    print("\nIn-block (#sandwiches, victims, profitA_SOL) by category:")
    print(
        agg_ib.to_string(
                index=False, formatters={"profitA_SOL": lambda x: f"{x:.6f}"}
        )
    )

    x = np.arange(len(order))
    width = 0.25

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    b1 = ax1.bar(x - width, agg_ib["sandwiches"], width=width, label="#sandwiches")
    b2 = ax1.bar(x, agg_ib["victims"], width=width, label="victims")
    b3 = ax2.bar(
        x + width, agg_ib["profitA_SOL"], width=width, label="profitA (SOL)"
    )

    # Text labels on bars
    for i, v in enumerate(agg_ib["sandwiches"]):
        ax1.text(x[i] - width, v, f"{v}", ha="center", va="bottom")
    for i, v in enumerate(agg_ib["victims"]):
        ax1.text(x[i], v, f"{v}", ha="center", va="bottom")
    for i, v in enumerate(agg_ib["profitA_SOL"]):
        ax2.text(x[i] + width, v, f"{v:.2f}", ha="center", va="bottom")

    ax1.set_xticks(x)
    ax1.set_xticklabels(order)
    ax1.set_xlabel("Signer/Owner category")
    ax1.set_ylabel("Count (#sandwiches, victims)")
    ax2.set_ylabel("profitA (SOL)")
    ax1.set_title(
            f"In-block Sandwich Epoch {prev_epoch}: grouped metrics per category"
    )
    # Combined legend
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left")
    fig.tight_layout()
    plt.savefig(f"sandwich_grouped_inblock_epoch_{prev_epoch}.png", dpi=300)
    plt.close()

    # -------- Cross-block figure --------
    agg_cb = _aggregate_three_metrics(cb)
    print("\nCross-block (#sandwiches, victims, profitA_SOL) by category:")
    print(
        agg_cb.to_string(
            index=False, formatters={"profitA_SOL": lambda x: f"{x:.6f}"}
        )
    )

    x = np.arange(len(order))
    width = 0.25

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()

    b1 = ax1.bar(x - width, agg_cb["sandwiches"], width=width, label="#sandwiches")
    b2 = ax1.bar(x, agg_cb["victims"], width=width, label="victims")
    b3 = ax2.bar(
        x + width, agg_cb["profitA_SOL"], width=width, label="profitA (SOL)"
    )

    for i, v in enumerate(agg_cb["sandwiches"]):
        ax1.text(x[i] - width, v, f"{v}", ha="center", va="bottom")
    for i, v in enumerate(agg_cb["victims"]):
        ax1.text(x[i], v, f"{v}", ha="center", va="bottom")
    for i, v in enumerate(agg_cb["profitA_SOL"]):
        ax2.text(x[i] + width, v, f"{v:.2f}", ha="center", va="bottom")

    ax1.set_xticks(x)
    ax1.set_xticklabels(order)
    ax1.set_xlabel("Signer/Owner category")
    ax1.set_ylabel("Count (#sandwiches, victims)")
    ax2.set_ylabel("profitA (SOL)")
    ax1.set_title(
        f"Cross-block Sandwich Epoch {prev_epoch}: grouped metrics per category"
    )
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left")
    fig.tight_layout()
    plt.savefig(f"sandwich_grouped_crossblock_epoch_{prev_epoch}.png", dpi=300)
    plt.close()

    # Profit of different token
    # In-block
    # Keep rows with a valid tokenA and numeric profitA
    ib_token_profit = ib.loc[ib["tokenA"].notna(), ["tokenA", "profitA"]].copy()
    ib_token_profit["tokenA"] = ib_token_profit["tokenA"].astype(str)
    ib_token_profit["profitA"] = pd.to_numeric(
        ib_token_profit["profitA"], errors="coerce"
    ).fillna(0.0)

    profit_by_token_ib = (
        ib_token_profit.groupby("tokenA", as_index=False)
        .agg(total_profitA=("profitA", "sum"))
        .sort_values("total_profitA", ascending=False)
        .reset_index(drop=True)
    )

    print("\nTop 20 tokenA by total profitA (In-block):")
    print(
        profit_by_token_ib.head(20).to_string(
            index=False, formatters={"total_profitA": lambda x: f"{x:.6f}"}
        )
    )
    print("SOL, ", profit_by_token_ib["total_profitA"][profit_by_token_ib["tokenA"] == "SOL"].values)

    # Cross-block      
    cb_token_profit = cb.loc[cb["tokenA"].notna(), ["tokenA", "profitA"]].copy()
    cb_token_profit["tokenA"] = cb_token_profit["tokenA"].astype(str)
    cb_token_profit["profitA"] = pd.to_numeric(
        cb_token_profit["profitA"], errors="coerce"
    ).fillna(0.0)

    profit_by_token_cb = (
        cb_token_profit.groupby("tokenA", as_index=False)
        .agg(total_profitA=("profitA", "sum"))
        .sort_values("total_profitA", ascending=False)
        .reset_index(drop=True)
    )

    print("\nTop 20 tokenA by total profitA (Cross-block):")
    print(
        profit_by_token_cb.head(20).to_string(
            index=False, formatters={"total_profitA": lambda x: f"{x:.6f}"}
        )
    )
    print("SOL, ", profit_by_token_cb["total_profitA"][profit_by_token_cb["tokenA"] == "SOL"].values)
