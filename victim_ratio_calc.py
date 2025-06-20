import clickhouse_connect
import pandas as pd
import requests
from datetime import datetime
# 查询 ClickHouse 获取 victim_count 数据
client = clickhouse_connect.get_client(
    host='209.192.245.26',
    port=8123,
    username='sol',
    password='sol'
)

rpc_url="https://api.mainnet-beta.solana.com"
def get_epoch_info():
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo"
    }

    response = requests.post(rpc_url, json=payload)
    data = response.json()["result"]

    # 当前 epoch 信息
    current_epoch = data["epoch"]
    current_slot = data["absoluteSlot"]
    slot_index = data["slotIndex"]
    slots_in_epoch = data["slotsInEpoch"]

    # 当前 epoch 起始 slot
    current_epoch_start = current_slot - slot_index

    # 上一个 epoch 的信息
    prev_epoch = current_epoch - 1
    prev_epoch_start = current_epoch_start - slots_in_epoch
    prev_epoch_end = current_epoch_start - 1

    return  prev_epoch,prev_epoch_start,prev_epoch_end

def get_validator_info():
    # 请求 RPC 获取验证者信息
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getVoteAccounts"
    }
    response = requests.post(rpc_url, json=payload)
    vote_data = response.json()["result"]
    validators = vote_data["current"] + vote_data["delinquent"]
    return validators

def get_validator_info_from_clickhouse(epoch: int) -> pd.DataFrame:
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
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df

if __name__ == "__main__":
    prev_epoch,prev_epoch_start,prev_epoch_end = get_epoch_info()
    
    # 获取 上一个epoch 的 victim_count 
    query = f'''
    SELECT l.leader, count(*) AS victim_count
    FROM solwich.sandwiches_nobundle AS s
    ANY INNER JOIN solwich.slot_leaders AS l ON s.slot = l.slot
    WHERE
        s.slot >= {prev_epoch_start}
        AND s.slot <= {prev_epoch_end}
        AND s.type ='backrun'
    GROUP BY l.leader
    ORDER BY victim_count DESC
    '''
    result = client.query(query)
    victim_df = pd.DataFrame(result.result_rows, columns=result.column_names)
    victim_df.rename(columns={"leader": "validator_account"}, inplace=True)

    # 构建 validators DataFrame
    # 合并基础信息
    validators_df = get_validator_info_from_clickhouse(prev_epoch)
    final_df = pd.merge(validators_df, victim_df, on="validator_account", how="left")
    final_df["victim_count"] = final_df["victim_count"].fillna(0).astype(int)
    final_df["commission"] = final_df["commission"].round(2)
    final_df["total_stake"] = final_df["total_stake"].round(2)

    # 计算 weighted_victim_count
    total_stake_sum = final_df["total_stake"].sum()
    final_df["weighted_victim_count"] = (final_df["total_stake"] / total_stake_sum) * final_df["victim_count"]
    final_df["weighted_victim_count"] = final_df["weighted_victim_count"].round(4)

    # slot_sum 查询
    query_slot_sum = f'''
    SELECT leader AS validator_account, count(*) AS slot_sum
    FROM solwich.slot_leaders
    WHERE slot >= {prev_epoch_start}
    AND slot <= {prev_epoch_end}
    GROUP BY leader
    '''
    slot_sum_result = client.query(query_slot_sum)
    slot_sum_df = pd.DataFrame(slot_sum_result.result_rows, columns=slot_sum_result.column_names)

    # 合并并计算 victim_slot_ratio
    final_df = final_df.merge(slot_sum_df, on="validator_account", how="left")
    final_df["slot_sum"] = final_df["slot_sum"].fillna(0).astype(int)
    final_df["victim_slot_ratio"] = final_df.apply(
        lambda row: round(row["victim_count"] / row["slot_sum"], 4) if row["slot_sum"] > 0 else 0,
        axis=1
    )
    query_total_victim_count = f'''
    SELECT
        l.leader as validator_account,
        SUM(v.victim_count) AS total_victim_count
    FROM (
        SELECT
            slot,
            count(*) AS victim_count
        FROM solwich.sandwiches_nobundle
        WHERE
            type ='backrun'
            AND slot >= {prev_epoch_start}
            AND slot <= {prev_epoch_end}
        GROUP BY slot
    ) AS v
    INNER JOIN solwich.slot_leaders AS l
        ON v.slot = l.slot
    GROUP BY l.leader
    '''
    # 查询 total_victim_count
    total_victim_result = client.query(query_total_victim_count)
    total_victim_df = pd.DataFrame(total_victim_result.result_rows, columns=total_victim_result.column_names)

    # 合并 total_victim_count
    final_df = final_df.merge(total_victim_df, on="validator_account", how="left")
    final_df["total_victim_count"] = final_df["total_victim_count"].fillna(0).astype(int)

    final_df["avg_victim_count"] = final_df.apply(
        lambda row: round(row["total_victim_count"] / row["slot_sum"], 2) if row["slot_sum"] > 0 else 0,
        axis=1
    )

    final_df["epoch_num"] = prev_epoch
    # 输出列排序 & 重命名
    final_df = final_df.sort_values(by="victim_count", ascending=False)

    final_df = final_df[[
        "epoch_num", "validator_account", "vote_account", "commission", "total_stake",
        "victim_count", "total_victim_count", "avg_victim_count",
        "weighted_victim_count", "slot_sum", "victim_slot_ratio"
    ]]

    # 导出为 xcel
    # client.insert_df('solwich.epoch_validator_stats_inbundle_all', final_df)
    # print("数据已写入 ClickHouse 表 solwich.epoch_validator_stats_inbundle_all")

    # # 获取当前时间，格式为：2025_05_22_14-30
    current_time_str = datetime.now().strftime("%Y_%m_%d_%H-%M")

    # 构造文件名
    filename = f"epoch_validator_stats_inbundle_all_{current_time_str}.xlsx"

    # 导出 Excel
    final_df.to_excel(filename, index=False)
    print(f"文件已保存为 {filename}")