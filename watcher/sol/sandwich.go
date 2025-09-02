package sol

import (
	"fmt"
	"time"
	"watcher/config"
	"watcher/db"
	"watcher/logger"
	"watcher/types"
)

func RunSandwichCmd(startSlot uint64) error {
	ch := db.NewClickhouse()
	defer ch.Close()

	// TODO: refine startSlot logic
	currentSlotFromRpc, err := GetCurrentSlot()
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}
	logger.SolLogger.Info("Current slot from Solana RPC", "slot", currentSlotFromRpc)
	startSlot = currentSlotFromRpc - config.SOL_FETCH_SLOT_DATA_MAX_GAP
	logger.SolLogger.Info("Fetch slot data statistic", "start", startSlot, "current_from_rpc", currentSlotFromRpc)

	logger.SolLogger.Info("Syncing slot data from current slot", "current", currentSlotFromRpc)
	for {
		currentSlot, err := GetCurrentSlot()
		if err != nil {
			logger.SolLogger.Error("Failed to get current slot", "err", err)
			continue
		}

		numToFetch := config.SOL_FETCH_SLOT_DATA_SLOT_NUM
		if currentSlot-startSlot < config.SOL_FETCH_SLOT_DATA_SLOT_NUM {
			logger.SolLogger.Info("Not enough new slots, sleep and retry after "+config.SOL_FETCH_SLOT_DATA_INTERVAL.String(), "start", startSlot, "current", currentSlot)
			time.Sleep(config.SOL_FETCH_SLOT_DATA_INTERVAL)
			continue
		}

		// Fetch blocks
		logger.SolLogger.Info("Fetch slot data (start)", "start", startSlot, "current", currentSlot, "num_to_fetch", numToFetch)
		fetchTimeBefore := time.Now()
		blocks := GetBlocks(startSlot, uint64(numToFetch))
		fetchTime := time.Since(fetchTimeBefore)
		logger.SolLogger.Info("Fetched slot data (done)", "start", startSlot, "num_fetched", len(blocks), "fetch_time", fetchTime.String())

		// Test print
		fmt.Println("fetched blocks:")
		for _, b := range blocks {
			logger.SolLogger.Info("Block", "slot", b.Slot, "block_height", b.BlockHeight, "num_txs", len(b.Txs), "timestamp", b.Timestamp.String())
			// for i, tx := range b.Txs {
			// 	if i <= 3 {
			// 		logger.SolLogger.Info("  Tx", "slot", tx.Slot, "signature", tx.Signature, "signer", tx.Signer, "is_failed", tx.IsFailed, "num_account_keys", len(tx.AccountKeys), "programs", tx.Programs, "ata_owner", tx.AtaOwner, "balance_change", tx.BalanceChange)
			// 	}
			// }
		}

		// Process blocks to find sandwiches
		logger.SolLogger.Info("Process slot data (start)", "start", startSlot, "num_fetched", len(blocks))
		processTimeBefore := time.Now()
		ProcessBlocksForSandwich(blocks)
		processTime := time.Since(processTimeBefore)
		logger.SolLogger.Info("Processed slot data (done)", "start", startSlot, "num_fetched", len(blocks), "process_time", processTime.String())
		return nil // For testing, only fetch once

		// Update next start slot
		startSlot += uint64(len(blocks))
		// Sleep a while
		logger.SolLogger.Info("Sleeping for "+config.SOL_FETCH_SLOT_LEADER_LONG_INTERVAL.String(), "next_start", startSlot)
		time.Sleep(config.SOL_FETCH_SLOT_DATA_INTERVAL)
	}
}

func ProcessBlocksForSandwich(blocks types.Blocks) {

}
