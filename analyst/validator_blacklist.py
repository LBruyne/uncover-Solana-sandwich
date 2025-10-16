import clickhouse_connect
import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
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


def query_sandwiches_group_by_leader(start_slot: int, end_slot: int) -> pd.DataFrame:
    """
    Count sandwiches data per validator in the given slot range.
    """
    query = f"""
    SELECT
        l.leader,
        countDistinct(s.sandwichId) AS sandwich_count,               
        sum(s.victimCount) AS total_victim_count,      
        sumIf(s.profitA, s.tokenA = 'SOL') AS total_SOL_profit
    FROM solwich.sandwiches AS s
    INNER JOIN solwich.slot_leaders AS l ON s.slot = l.slot
    WHERE s.slot BETWEEN {start_slot} AND {end_slot}
    GROUP BY l.leader
    ORDER BY total_SOL_profit DESC
    """
    result = client.query(query)
    res = pd.DataFrame(result.result_rows, columns=result.column_names)
    res.rename(columns={"leader": "validator"}, inplace=True)
    return res


def query_sandwiches_with_txs_and_leader(
    start_slot: int, end_slot: int
) -> pd.DataFrame:
    """
    Step 1:
    - From solwich.sandwiches, fetch all sandwiches in [start_slot, end_slot].
    - Ensure each sandwichId appears at most once using LIMIT 1 BY sandwichId.
    Step 2:
    - Fetch all sandwiches' txs (*) from solwich.sandwich_txs.
    - Deduplicate txs where (type, signature) are the same
    - Return the integrated rows: sandwich columns (*) + tx columns (*).
    Step 3:
    - Join slot_leaders on each tx's slot to add leader for every row.
    """
    query = f"""
    WITH dedup_sandwiches AS
    (
        SELECT
            sandwichId, slot as sandwich_slot, crossBlock, tokenA, tokenB, signerSame, ownerSame, profitA, relativeDiffB
        FROM solwich.sandwiches
        WHERE slot BETWEEN {start_slot} AND {end_slot}
        ORDER BY slot DESC, sandwichId DESC
        LIMIT 1 BY sandwichId
    ),
    dedup_txs AS
    (
        SELECT
            *
        FROM solwich.sandwich_txs
        WHERE sandwichId IN (SELECT sandwichId FROM dedup_sandwiches) AND slot BETWEEN {start_slot} AND {end_slot}
        ORDER BY slot DESC
        LIMIT 1 BY type, signature
    ),
    s_leaders AS 
    (
        SELECT
            *
        FROM solwich.slot_leaders
        WHERE slot BETWEEN {start_slot} AND {end_slot}
        ORDER BY slot DESC
    )
    SELECT
        s.sandwichId as sandwichId,
        s.sandwich_slot,
        l.leader,
        s.crossBlock,
        s.tokenA,
        s.tokenB,
        s.signerSame,
        s.ownerSame,
        s.profitA,
        s.relativeDiffB,
        t.type,
        t.slot as tx_slot,
        t.position,
        t.timestamp,
        t.fee,
        t.signature,
        t.signer,
        t.inBundle,
        t.programs,
        t.fromToken,
        t.toToken,
        t.fromAmount,
        t.toAmount,
        t.attackerPreBalanceB,
        t.attackerPostBalanceB,
        t.ownersOfB,
        t.fromTotalAmount,
        t.toTotalAmount,
        t.diffA,
        t.diffB
    FROM dedup_sandwiches AS s
    INNER JOIN dedup_txs AS t
        ON t.sandwichId = s.sandwichId
    LEFT JOIN s_leaders AS l
        ON l.slot = t.slot
    ORDER BY sandwich_slot DESC
    """
    result = client.query(query)
    res = pd.DataFrame(result.result_rows, columns=result.column_names)
    res.rename(columns={"leader": "validator"}, inplace=True)
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

    leader_slots = query_leader_slot_counts(prev_epoch_start, prev_epoch_end)
    print(
        f"Total slots led in epoch {prev_epoch} (slots {prev_epoch_start} to {prev_epoch_end}):"
    )
    print(leader_slots.head(10))
    print(
        "Total slots:",
        leader_slots["slot_count"].sum(),
        "Total actual slots checked:",
        leader_slots["actual_slot_count"].sum(),
    )

    # Query victims number, sandwich number and SOL profit per validator in the previous epoch
    sandwiches_by_leader = query_sandwiches_group_by_leader(
        prev_epoch_start, prev_epoch_end
    )
    print(sandwiches_by_leader.head(10))
    print("Total sandwiches:", sandwiches_by_leader["sandwich_count"].sum())
    print("Total profit in SOL:", sandwiches_by_leader["total_SOL_profit"].sum())
    print("Total victims:", sandwiches_by_leader["total_victim_count"].sum())
    # Compute victims per slot, sandwiches per slot, profit per slot
    leader_statistic = leader_slots.merge(
        sandwiches_by_leader, on="validator", how="left"
    )
    leader_statistic["victims_per_slot"] = leader_statistic.apply(
        lambda row: (
            row["total_victim_count"] / row["actual_slot_count"]
            if row["actual_slot_count"] > 0
            else 0
        ),
        axis=1,
    )
    leader_statistic["sandwiches_per_slot"] = leader_statistic.apply(
        lambda row: (
            row["sandwich_count"] / row["actual_slot_count"]
            if row["actual_slot_count"] > 0
            else 0
        ),
        axis=1,
    )
    leader_statistic["profit_per_slot"] = leader_statistic.apply(
        lambda row: (
            row["total_SOL_profit"] / row["actual_slot_count"]
            if row["actual_slot_count"] > 0
            else 0
        ),
        axis=1,
    )
    leader_statistic = leader_statistic.sort_values(
        by=["sandwiches_per_slot", "profit_per_slot", "victims_per_slot"],
        ascending=False,
    )
    print("Leader statistic with victims/slot, sandwiches/slot, profit/slot:")
    print(leader_statistic.head(10))

    # Slots with sandwiches and their txs
    sandwiches_txs = query_sandwiches_with_txs_and_leader(
        prev_epoch_start, prev_epoch_end
    )
    print(
        f"Total rows of sandwiches with txs and leader in epoch {prev_epoch}: sandwiches - {sandwiches_by_leader['sandwich_count'].sum()} sandwich_txs - {len(sandwiches_txs)}"
    )

    # Jito bundle coverage analysis
    jito_view = sandwiches_txs.copy()
    agg = jito_view.groupby("sandwichId", as_index=False).agg(
        tx_count=("signature", "count"),
        inbundle_count=("inBundle", lambda x: x.fillna(False).astype(int).sum()),
    )

    def _classify(row):
        if row["tx_count"] == 0:
            return "no_tx"
        if row["inbundle_count"] == 0:
            return "none_in_bundle"
        if row["inbundle_count"] == row["tx_count"]:
            return "all_in_bundle"
        return "partial_in_bundle"

    agg["bundle_status"] = agg.apply(_classify, axis=1)
    summary = (
        agg[agg["bundle_status"] != "no_tx"]
        .groupby("bundle_status", dropna=False)
        .size()
        .reset_index(name="sandwich_count")
        .sort_values("sandwich_count", ascending=False)
        .reset_index(drop=True)
    )
    total_with_tx = int(summary["sandwich_count"].sum())
    summary["share"] = (
        summary["sandwich_count"] / total_with_tx if total_with_tx > 0 else 0.0
    )
    print(
        f"Jito bundle coverage in epoch {prev_epoch} (slots {prev_epoch_start} to {prev_epoch_end}):"
    )
    print(summary)

    # Distance analysis
    distance_view = sandwiches_txs.copy()

    def bundle_status(row_tx_count, row_inbundle_count):
        if row_tx_count == 0:
            return "no_tx"
        if row_inbundle_count == 0:
            return "none_in_bundle"
        if row_inbundle_count == row_tx_count:
            return "all_in_bundle"
        return "partial_in_bundle"

    dist_rows = []
    for sid, g in distance_view.groupby("sandwichId"):
        cross_block = bool(g["crossBlock"].iloc[0])
        tx_count = int(len(g))
        inbundle_count = int(g["inBundle"].sum())
        status = bundle_status(tx_count, inbundle_count)

        # Consecutive
        pos_unique = sorted(g["position"].dropna().unique().tolist())
        consecutive = False
        if not cross_block and len(pos_unique) > 0:
            consecutive = pos_unique[-1] - pos_unique[0] + 1 == len(pos_unique)

        # Distance
        fr = g[g["type"] == "frontRun"]
        br = g[g["type"] == "backRun"]
        last_front_pos = fr["position"].max() if not fr.empty else None
        first_back_pos = br["position"].min() if not br.empty else None
        last_front_slot = fr["tx_slot"].max() if not fr.empty else None
        first_back_slot = br["tx_slot"].min() if not br.empty else None

        inblock_distance = None
        if (
            not cross_block
            and last_front_pos is not None
            and first_back_pos is not None
        ):
            inblock_distance = int(first_back_pos - last_front_pos)

        crossblock_gap_slots = None
        if (
            cross_block
            and (last_front_slot is not None)
            and (first_back_slot is not None)
        ):
            crossblock_gap_slots = int(first_back_slot - last_front_slot)

        dist_rows.append(
            {
                "sandwichId": sid,
                "cross_block": cross_block,
                "consecutive": bool(consecutive),
                "bundle_status": status,  # all/partial/none/no_tx
                "tx_count": tx_count,
                "inbundle_count": inbundle_count,
                "inblock_distance": inblock_distance,
                "crossblock_gap_slots": crossblock_gap_slots,
            }
        )

    dist_stat_df = pd.DataFrame(dist_rows)

    # -------- 1) in_block 且所有交易“紧贴在一起”的三明治 & Jito 覆盖 --------
    inblock_consecutive = dist_stat_df[
        (~dist_stat_df["cross_block"]) & (dist_stat_df["consecutive"])
    ]
    print("\n[1] In-block & consecutive sandwiches")
    print(f"Count: {len(inblock_consecutive)}")
    cov1 = (
        inblock_consecutive[inblock_consecutive["bundle_status"] != "no_tx"]
        .groupby("bundle_status")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    total1 = int(cov1["count"].sum()) if not cov1.empty else 0
    cov1["share"] = cov1["count"] / total1
    print("Jito bundle coverage (all/partial/none):")
    print(cov1)

    # -------- 2) in_block 但“非连续”的三明治：distance 统计 & 覆盖 --------
    inblock_non_contig = dist_stat_df[
        (~dist_stat_df["cross_block"]) & (~dist_stat_df["consecutive"])
    ]
    print("\n[2] In-block & non-contiguous sandwiches")
    print(f"Count: {len(inblock_non_contig)}")
    dist = inblock_non_contig["inblock_distance"].dropna().astype(int)
    print("Distance summary (first_backrun.position - last_frontrun.position):")
    print(dist.describe())
    print("Top distances (value counts):")
    print(dist.value_counts().head(10))

    cov2 = (
        inblock_non_contig[inblock_non_contig["bundle_status"] != "no_tx"]
        .groupby("bundle_status")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    total2 = int(cov2["count"].sum()) if not cov2.empty else 0
    cov2["share"] = cov2["count"] / total2
    print("Jito bundle coverage (all/partial/none):")
    print(cov2)

    # -------- 3) cross_block 数量 & front/back 相隔 slot 数 & 覆盖 --------
    cross_block_df = dist_stat_df[dist_stat_df["cross_block"]]
    print("\n[3] Cross-block sandwiches")
    print(f"Count: {len(cross_block_df)}")
    gaps = cross_block_df["crossblock_gap_slots"].dropna().astype(int)
    print("Gap slots summary (min backrun slot - max frontrun slot):")
    print(gaps.describe())
    print("Top gap slots (value counts):")
    print(gaps.value_counts().head(10))

    cov3 = (
        cross_block_df[cross_block_df["bundle_status"] != "no_tx"]
        .groupby("bundle_status")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    total3 = int(cov3["count"].sum()) if not cov3.empty else 0
    cov3["share"] = cov3["count"] / total3
    print("Jito bundle coverage (all/partial/none):")
    print(cov3)
