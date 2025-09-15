package sol

import (
	"fmt"
	"sort"
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

		// Test print block
		for _, b := range blocks {
			types.PPBlock(b, 1)
		}

		// Process blocks to find sandwiches
		logger.SolLogger.Info("Process slot data (start)", "start", startSlot, "num_fetched", len(blocks))
		timeProess := time.Now()
		inBlockSandwiches, crossBlockSandwiches := ProcessBlocksForSandwich(blocks)
		logger.SolLogger.Info("Process slot data (done)", "start", startSlot, "num_in_block_sandwiches", len(inBlockSandwiches), "num_cross_block_sandwiches", len(crossBlockSandwiches), "process_time", time.Since(timeProess).String())

		// Test print sandwiches
		for i, s := range inBlockSandwiches {
			types.PPInBlockSandwich(i+1, s)
		}
		for i, s := range crossBlockSandwiches {
			types.PPCrossBlockSandwich(i+1, s)
		}

		// Update next start slot
		startSlot += uint64(len(blocks))
		// Sleep a while
		logger.SolLogger.Info("Sleeping for "+config.SOL_FETCH_SLOT_LEADER_LONG_INTERVAL.String(), "next_start", startSlot)
		time.Sleep(config.SOL_FETCH_SLOT_DATA_INTERVAL)
		return nil // For testing, only fetch once
	}
}

func ProcessBlocksForSandwich(blocks types.Blocks) (inBlock []*types.InBlockSandwich, crossBlock []*types.CrossBlockSandwich) {
	var wg sync.WaitGroup
	inCh := make(chan []*types.InBlockSandwich, 1)
	crCh := make(chan []*types.CrossBlockSandwich, 1)
	wg.Add(1)
	go func() { inCh <- ProcessInBlockSandwich(blocks, &wg) }()
	wg.Add(1)
	go func() { crCh <- ProcessCrossBlockSandwich(blocks, &wg) }()
	wg.Wait()

	close(inCh)
	close(crCh)

	if v, ok := <-inCh; ok {
		inBlock = v
	}
	if v, ok := <-crCh; ok {
		crossBlock = v
	}

	return
}

func ProcessInBlockSandwich(blocks types.Blocks, wg *sync.WaitGroup) []*types.InBlockSandwich {
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
	sandwiches := make([]*types.InBlockSandwich, 0)
	for sandwich := range sandwichesCh {
		sandwiches = append(sandwiches, sandwich...)
	}

	sort.Slice(sandwiches, func(i, j int) bool {
		if sandwiches[i].Slot != sandwiches[j].Slot {
			return sandwiches[i].Slot < sandwiches[j].Slot
		}
		return sandwiches[i].Timestamp.Before(sandwiches[j].Timestamp)
	})

	return sandwiches
}

func FindInBlockSandwiches(b *types.Block) []*types.InBlockSandwich {
	finder := &InBlockSandwichFinder{
		Txs:             b.Txs,
		AmountThreshold: config.SANDWICH_AMOUNT_THRESHOLD,
	}

	timeFind := time.Now()
	finder.Find()
	logger.SolLogger.Info("Find in-block sandwiches", "slot", b.Slot, "num_txs", len(b.Txs), "num_sandwiches", len(finder.Sandwiches), "time_cost", time.Since(timeFind).String())
	return finder.Sandwiches
}

func ProcessCrossBlockSandwich(blocks types.Blocks, waitGroup *sync.WaitGroup) []*types.CrossBlockSandwich {
	defer waitGroup.Done()
	return nil
}
