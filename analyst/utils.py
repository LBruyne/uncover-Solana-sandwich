import requests

solana_rpc_url = "https://api.mainnet-beta.solana.com"


def fetch_prev_epoch_info():
    """
    Fetch Solana epoch information via JSON-RPC and compute the previous epoch's ID and slot range.
    """
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getEpochInfo"}
    response = requests.post(solana_rpc_url, json=payload)
    data = response.json()["result"]
    print(
        f"Current epoch: {data['epoch']}, Slot: {data['absoluteSlot']}, Slot index in epoch: {data['slotIndex']}, Slots in epoch: {data['slotsInEpoch']}"
    )

    current_epoch = data["epoch"]
    current_slot = data["absoluteSlot"]
    slot_index = data["slotIndex"]
    slots_in_epoch = data["slotsInEpoch"]

    current_epoch_start = current_slot - slot_index
    prev_epoch = current_epoch - 1
    prev_epoch_start = current_epoch_start - slots_in_epoch
    prev_epoch_end = current_epoch_start - 1

    return prev_epoch, prev_epoch_start, prev_epoch_end
