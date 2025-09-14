package sol

import (
	"fmt"
	"sync"
	"time"
	"watcher/config"
	"watcher/db"
	"watcher/logger"
	"watcher/types"
)

var ch db.Database

func RunSandwichCmd(startSlot uint64) error {
	ch = db.NewClickhouse()
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
			for i, tx := range b.Txs {
				if i <= 1 {
					logger.SolLogger.Info("Tx", "slot", tx.Slot, "signature", tx.Signature, "signer", tx.Signer, "is_failed", tx.IsFailed, "is_vote", tx.IsVote, "num_account_keys", len(tx.AccountKeys), "ata_owner", tx.AtaOwner, "owner_balance_changes", tx.OwnerBalanceChanges, "related_tokens", tx.RelatedTokens.ToSlice(), "related_pools", tx.RelatedPools.ToSlice(), "related_pools_info", tx.RelatedPoolsInfo)
				}
			}
		}

		// Process blocks to find sandwiches
		logger.SolLogger.Info("Process slot data (start)", "start", startSlot, "num_fetched", len(blocks))
		processTimeBefore := time.Now()
		ProcessBlocksForSandwich(blocks)
		processTime := time.Since(processTimeBefore)
		logger.SolLogger.Info("Processed slot data (done)", "start", startSlot, "num_fetched", len(blocks), "process_time", processTime.String())

		// Update next start slot
		startSlot += uint64(len(blocks))
		// Sleep a while
		logger.SolLogger.Info("Sleeping for "+config.SOL_FETCH_SLOT_LEADER_LONG_INTERVAL.String(), "next_start", startSlot)
		time.Sleep(config.SOL_FETCH_SLOT_DATA_INTERVAL)
		return nil // For testing, only fetch once
	}
}

func ProcessBlocksForSandwich(blocks types.Blocks) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { ProcessInBlockSandwich(blocks, &wg) }()
	wg.Add(1)
	go func() { ProcessCrossBlockSandwich(blocks, &wg) }()
	wg.Wait()
}

func ProcessInBlockSandwich(blocks types.Blocks, wg *sync.WaitGroup) {
	defer wg.Done()

	// Process in-block sandwiches in parallel
	parallel := config.SOL_PROCESS_IN_BLOCK_SANDWICH_PARALLEL_NUM
	blocksQueue := make(chan *types.Block, len(blocks))
	sandwichesCh := make(chan []*types.InBlockSandwich)

	var processWg sync.WaitGroup

	// Initialize blocks queue
	go func() {
		for _, b := range blocks {
			blocksQueue <- b
		}
		// Close channel after all blocks are sent
		close(blocksQueue)
	}()

	processWg.Add(parallel)
	for range parallel {
		go func() {
			defer processWg.Done()
			// Worker goroutine to process blocks after all blocks are sent
			for b := range blocksQueue {
				// Process in-block sandwiches
				sandwiches := FindInBlockSandwiches(b)
				// Send found sandwiches to channel
				sandwichesCh <- sandwiches
			}
		}()
	}

	// Close sandwiches channel when all processing goroutines are done
	go func() {
		processWg.Wait()
		close(sandwichesCh)
	}()

	// Collect sandwiches from channel
	sandwiches := make([]any, 0)
	for sandwich := range sandwichesCh {
		sandwiches = append(sandwiches, sandwich)
	}
	// Sort queued sandwiches by position

	// Insert in-block sandwiches into DB
	timeDB := time.Now()
	// err := ch.InsertSandwiches(sandwiches)
	// if err != nil {
	// 	logger.SolLogger.Error("Failed to insert in-block sandwiches into DB", "err", err)
	// } else {
	// 	logger.SolLogger.Info("Inserted in-block sandwiches into DB", "num_sandwiches", len(sandwiches), "time_cost", time.Since(timeDB).String())
	// }
	logger.SolLogger.Info("Insert sandwiches", "num_sandwiches", len(sandwiches), "time_cost", time.Since(timeDB).String())
}

func FindInBlockSandwiches(b *types.Block) []*types.InBlockSandwich {
	finder := &InBlockSandwichFinder{
		Txs:             b.Txs,
		AmountThreshold: config.SANDWICH_AMOUNT_THRESHOLD,
	}

	finder.Find()
	return finder.Sandwiches
}

func ProcessCrossBlockSandwich(blocks types.Blocks, waitGroup *sync.WaitGroup) {
	// panic("unimplemented")
}
