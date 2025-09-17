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

	currentSlot, err := GetCurrentSlot()
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}
	logger.SolLogger.Info("Current slot from Solana RPC", "slot", currentSlot)

	if startSlot < currentSlot-config.SOL_FETCH_SLOT_DATA_MAX_GAP || startSlot > currentSlot {
		startSlot = currentSlot - config.SOL_FETCH_SLOT_DATA_MAX_GAP
	}
	logger.SolLogger.Info("Fetch slot setting adjusts", "start", startSlot, "current_from_rpc", currentSlot)

	logger.SolLogger.Info("Syncing slot data start from", "start", startSlot)
	for {
		currentSlot, err := GetCurrentSlot()
		if err != nil {
			logger.SolLogger.Error("Failed to get current slot", "err", err)
			continue
		}

		if startSlot < currentSlot-config.SOL_FETCH_SLOT_DATA_MAX_GAP {
			startSlot = currentSlot - config.SOL_FETCH_SLOT_DATA_MAX_GAP
			logger.SolLogger.Info("Start slot too old, adjust to", "start", startSlot, "current", currentSlot-config.SOL_FETCH_SLOT_DATA_MAX_GAP)
		}

		numToFetch := config.SOL_FETCH_SLOT_DATA_SLOT_NUM
		if currentSlot-startSlot < config.SOL_FETCH_SLOT_DATA_SLOT_NUM {
			logger.SolLogger.Info("Not enough new slots, sleep and retry after "+config.SOL_FETCH_SLOT_DATA_LONG_INTERVAL.String(), "start", startSlot, "current", currentSlot)
			time.Sleep(config.SOL_FETCH_SLOT_DATA_LONG_INTERVAL)
			continue
		}

		// Fetch blocks
		logger.SolLogger.Info("Fetch slot data (start)", "start", startSlot, "current", currentSlot, "diff", currentSlot-startSlot, "num_to_fetch", numToFetch)
		fetchTimeBefore := time.Now()
		blocks := GetBlocks(startSlot, uint64(numToFetch))
		fetchTime := time.Since(fetchTimeBefore)
		logger.SolLogger.Info("Fetched slot data (done)", "start", startSlot, "num_fetched", len(blocks), "fetch_time", fetchTime.String())

		// Test print block
		// for _, b := range blocks {
		// 	types.PPBlock(b, 1)
		// }

		// Process blocks to find sandwiches
		logger.SolLogger.Info("Process slot data (start)", "start", startSlot, "num_fetched", len(blocks))
		timeProess := time.Now()
		inBlockSandwiches, crossBlockSandwiches := ProcessBlocksForSandwich(blocks)
		logger.SolLogger.Info("Process slot data (done)", "start", startSlot, "num_in_block_sandwiches", len(inBlockSandwiches), "num_cross_block_sandwiches", len(crossBlockSandwiches), "process_time", time.Since(timeProess).String())

		// Test print sandwiches
		// for i, s := range inBlockSandwiches {
		// 	types.PPInBlockSandwich(i+1, s)
		// }
		// for i, s := range crossBlockSandwiches {
		// 	types.PPCrossBlockSandwich(i+1, s)
		// }

		// Save to DB
		logger.SolLogger.Info("Store sandwiches related information to DB (start)")
		timeStore := time.Now()
		if err := StoreSandwichesToDB(ch, inBlockSandwiches, crossBlockSandwiches); err != nil {
			logger.SolLogger.Error("Failed to store sandwiches to DB", "err", err)
		}
		if err := StoreSlotSandwichStatusToDB(ch, blocks, inBlockSandwiches, crossBlockSandwiches); err != nil {
			logger.SolLogger.Error("Failed to store slot sandwich status to DB", "err", err)
		}
		logger.SolLogger.Info("Store sandwiches related information to DB (done)", "store_time", time.Since(timeStore).String())

		// Update next start slot
		startSlot += uint64(numToFetch)
		// Sleep a while
		logger.SolLogger.Info("Sleeping for "+config.SOL_FETCH_SLOT_DATA_SHORT_INTERVAL.String(), "next_start", startSlot)
		time.Sleep(config.SOL_FETCH_SLOT_DATA_SHORT_INTERVAL)
	}
}

func ProcessBlocksForSandwich(blocks types.Blocks) (inBlock []*types.InBlockSandwich, crossBlock []*types.CrossBlockSandwich) {
	var wg sync.WaitGroup
	inCh := make(chan []*types.InBlockSandwich, 1)
	crCh := make(chan []*types.CrossBlockSandwich, 1)
	wg.Add(2)
	go func() {
		defer wg.Done()
		inCh <- ProcessInBlockSandwich(blocks)
	}()
	go func() {
		defer wg.Done()
		crCh <- ProcessCrossBlockSandwich(blocks)
	}()

	inBlock = <-inCh
	crossBlock = <-crCh

	wg.Wait()

	return
}

func ProcessInBlockSandwich(blocks types.Blocks) []*types.InBlockSandwich {
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

func ProcessCrossBlockSandwich(blocks types.Blocks) []*types.CrossBlockSandwich {
	return nil
}

func StoreSandwichesToDB(ch db.Database, inBlockSandwiches []*types.InBlockSandwich, crossBlockSandwiches []*types.CrossBlockSandwich) error {
	if len(inBlockSandwiches) > 0 {
		if err := ch.InsertInBlockSandwiches(inBlockSandwiches); err != nil {
			return fmt.Errorf("failed to insert in-block sandwiches to DB: %w", err)
		}
		logger.SolLogger.Info("Inserted in-block sandwiches to DB", "num", len(inBlockSandwiches))

		sandwichTxToInsert := make([]*types.SandwichTx, 0)
		for _, s := range inBlockSandwiches {
			sandwichTxToInsert = append(sandwichTxToInsert, s.FrontRun...)
			sandwichTxToInsert = append(sandwichTxToInsert, s.Victims...)
			sandwichTxToInsert = append(sandwichTxToInsert, s.BackRun...)
		}
		if err := ch.InsertSandwichTxs(sandwichTxToInsert); err != nil {
			return fmt.Errorf("failed to insert in-block sandwich txs to DB: %w", err)
		}
		logger.SolLogger.Info("Inserted in-block sandwich txs to DB", "num", len(sandwichTxToInsert))
	}

	if len(crossBlockSandwiches) > 0 {
		// TODO: implement cross-block sandwich storage
		logger.SolLogger.Info("Cross-block sandwich storage not implemented yet", "num", len(crossBlockSandwiches))
	}

	return nil
}

func StoreSlotSandwichStatusToDB(ch db.Database, blks types.Blocks, inBlockSandwiches []*types.InBlockSandwich, crossBlockSandwiches []*types.CrossBlockSandwich) error {
	// Map slot to number of sandwich txs
	slotToSandwichTxCount := make(map[uint64]uint64)
	slotToSandwichCount := make(map[uint64]uint64)
	slotToSandwichVictimCount := make(map[uint64]uint64)
	// In-block sandwiches
	for _, s := range inBlockSandwiches {
		slotToSandwichTxCount[s.Slot] += uint64(len(s.FrontRun) + len(s.BackRun))
		slotToSandwichCount[s.Slot] += 1
		slotToSandwichVictimCount[s.Slot] += uint64(len(s.Victims))
	}
	// Cross-block sandwiches
	// TODO: Add cross-block sandwich tx num to corresponding slots

	statuses := make([]*types.SlotTxsStatus, 0, len(blks))
	for _, blk := range blks {
		statuses = append(statuses, &types.SlotTxsStatus{
			Slot:                    blk.Slot,
			TxFetched:               true,
			TxCount:                 uint64(len(blk.Txs)),
			ValidTxCount:            blk.ValidTxCount,
			SandwichFetched:         true,
			SandwichTxCount:         slotToSandwichTxCount[blk.Slot],
			SandwichCount:           slotToSandwichCount[blk.Slot],
			SandwichVictimCount:     slotToSandwichVictimCount[blk.Slot],
			SandwichInBundleChecked: false,
		})
	}

	if err := ch.InsertSlotTxs(statuses); err != nil {
		return fmt.Errorf("InsertSlotTxsStatus failed: %w", err)
	}

	return nil
}
