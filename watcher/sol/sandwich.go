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
var crossBlockCache = NewBlockCache(config.CROSS_BLOCK_CACHE_SIZE)

var totalBlock = uint64(0)
var totalTx = uint64(0)
var totalValidTx = uint64(0)
var totalTxInCrossBlockDetection = uint64(0)
var totalValidTxInCrossBlockDetection = uint64(0)
var totalSwapTxInCrossBlockDetection = uint64(0)
var totalTxInBucketInCrossBlockDetection = uint64(0)
var totalBucketInCrossBlockDetection = uint64(0)
var totalCrossBlockDetectionTimes = uint64(0)
var totalFetchTime = time.Duration(0)
var totalProcessTime = time.Duration(0)
var totalInBlockTime = time.Duration(0)
var totalCrossBlockTime = time.Duration(0)
var totalOldInBlockTime = time.Duration(0)
var BLOCK_IN_ONE_RUN = uint64(1000)

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
		if startSlot%config.PER_LEADER_SLOT != 0 {
			// Move to next slot which is multiple of 4
			startSlot += 1
			continue
		}

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
		if currentSlot-startSlot < config.SOL_FETCH_SLOT_LEADER_LIMIT {
			// logger.SolLogger.Info("Not enough new slots, sleep and retry after "+config.SOL_FETCH_SLOT_DATA_LONG_INTERVAL.String(), "start", startSlot, "current", currentSlot)
			time.Sleep(config.SOL_FETCH_SLOT_DATA_LONG_INTERVAL)
			continue
		}

		// Fetch blocks
		// logger.SolLogger.Info("Fetch slot data (start)", "start", startSlot, "current", currentSlot, "diff", currentSlot-startSlot, "num_to_fetch", numToFetch)
		fetchTimeBefore := time.Now()
		blocks := GetBlocks(startSlot, uint64(numToFetch))
		fetchTime := time.Since(fetchTimeBefore)
		// logger.SolLogger.Info("Fetched slot data (done)", "start", startSlot, "num_fetched", len(blocks), "fetch_time", fetchTime.String())

		for _, b := range blocks {
			for _, tx := range b.Txs {
				totalTx += 1
				if !tx.IsFailed && !tx.IsVote {
					totalValidTx += 1
				}
			}
		}

		// Process blocks to find sandwiches
		// logger.SolLogger.Info("Process sandwiches (start)", "start", startSlot, "num_fetched", len(blocks))
		timeProcessBefore := time.Now()
		timeProcessInBlockBefore := time.Now()
		ProcessInBlockSandwich(blocks)
		timeProcessInBlock := time.Since(timeProcessInBlockBefore)
		// logger.SolLogger.Info("Process in-block sandwiches (done)", "start", startSlot, "process_time", time.Since(timeProcessInBlock).String())

		// logger.SolLogger.Info("Process cross-block sandwiches (start)", "start", startSlot, "num_fetched", len(blocks))
		timeProcessCrossBlockBefore := time.Now()
		ProcessCrossBlockSandwich(blocks)
		timeProcessCrossBlock := time.Since(timeProcessCrossBlockBefore)
		// logger.SolLogger.Info("Process cross-block sandwiches (done)", "start", startSlot, "process_time", time.Since(timeProcessCross).String())

		// logger.SolLogger.Info("Old Process in-block sandwiches (start)", "start", startSlot, "num_fetched", len(blocks))
		timeProcessOldInBlockBefore := time.Now()
		OldProcessInBlockSandwich(blocks)
		timeProcessOldInBlock := time.Since(timeProcessOldInBlockBefore)
		timeProcess := time.Since(timeProcessBefore)

		totalBlock += uint64(len(blocks))
		totalInBlockTime += timeProcessInBlock
		totalCrossBlockTime += timeProcessCrossBlock
		totalOldInBlockTime += timeProcessOldInBlock
		totalProcessTime += timeProcess
		totalFetchTime += fetchTime

		// logger.SolLogger.Info("Process sandwiches (done)", "start", startSlot, "process_time", time.Since(timeProcessOldInBlock).String(), "total_blocks_this_time", totalBlock)

		// Test print sandwiches
		// for _, s := range inBlockSandwiches {
		// 	if s.TokenA != "SOL" {
		// 		continue
		// 	}

		// 	transferFound := false
		// 	for _, tx := range s.FrontRun {
		// 		if tx.Type == "transfer" {
		// 			transferFound = true
		// 			break
		// 		}
		// 	}
		// 	if transferFound {
		// 		logger.SolLogger.Info("Found in-block sandwich with transfer")
		// 		// types.PPInBlockSandwich(i+1, s)
		// 		for _, tx := range s.FrontRun {
		// 			logger.SolLogger.Info("  FrontRun", "tx", tx.Signature, "type", tx.Type, "signer", tx.Signer)
		// 		}
		// 		for _, tx := range s.BackRun {
		// 			logger.SolLogger.Info("  BackRun ", "tx", tx.Signature, "type", tx.Type, "signer", tx.Signer)
		// 		}
		// 	}
		// }
		// for _, s := range crossBlockSandwiches {
		// 	if s.TokenA != "SOL" {
		// 		continue
		// 	}

		// 	transferFound := false
		// 	for _, tx := range s.FrontRun {
		// 		if tx.Type == "transfer" {
		// 			transferFound = true
		// 			break
		// 		}
		// 	}
		// 	if transferFound {
		// 		logger.SolLogger.Info("Found cross-block sandwich with transfer")
		// 		// types.PPCrossBlockSandwich(i+1, s)
		// 		for _, tx := range s.FrontRun {
		// 			logger.SolLogger.Info("  FrontRun", "tx", tx.Signature, "type", tx.Type, "signer", tx.Signer)
		// 		}
		// 		for _, tx := range s.BackRun {
		// 			logger.SolLogger.Info("  BackRun ", "tx", tx.Signature, "type", tx.Type, "signer", tx.Signer)
		// 		}
		// 	}
		// }

		// Save to DB
		// logger.SolLogger.Info("Store sandwiches related information to DB (start)")
		// timeStore := time.Now()
		// if err := StoreSandwichesToDB(ch, inBlockSandwiches, crossBlockSandwiches); err != nil {
		// 	logger.SolLogger.Error("Failed to store sandwiches to DB", "err", err)
		// }
		// if err := StoreSlotSandwichStatusToDB(ch, blocks, inBlockSandwiches, crossBlockSandwiches); err != nil {
		// 	logger.SolLogger.Error("Failed to store slot sandwich status to DB", "err", err)
		// }
		// logger.SolLogger.Info("Store sandwiches related information to DB (done)", "store_time", time.Since(timeStore).String())

		if totalBlock >= BLOCK_IN_ONE_RUN {
			logger.SolLogger.Info("Reached benchmark block number:", "block_num", BLOCK_IN_ONE_RUN)
			logger.SolLogger.Info("Benchmark:", "start_slot", startSlot-totalBlock,
				"total_slots", totalBlock,
				"total_txs", totalTx,
				"total_valid_txs", totalValidTx,
				"average_txs_per_slot", totalTx/totalBlock,
				"average_valid_txs_per_slot", totalValidTx/totalBlock,
				"total_cross_block_detection_times", totalCrossBlockDetectionTimes,
				"average_txs_per_cross_block_times", totalTxInCrossBlockDetection/totalCrossBlockDetectionTimes,
				"average_valid_txs_per_cross_block_times", totalValidTxInCrossBlockDetection/totalCrossBlockDetectionTimes,
				"average_swap_txs_per_cross_block_times", totalSwapTxInCrossBlockDetection/totalCrossBlockDetectionTimes,
				"average_buckets_per_cross_block_times", totalBucketInCrossBlockDetection/totalCrossBlockDetectionTimes,
				"average_tx_in_buckets_per_cross_block_times", totalTxInBucketInCrossBlockDetection/totalCrossBlockDetectionTimes,
				"total_inblock_time", totalInBlockTime.String(),
				"total_crossblock_time", totalCrossBlockTime.String(),
				"total_old_inblock_time", totalOldInBlockTime.String(),
				"total_fetch_time", totalFetchTime.String(),
				"total_process_time", totalProcessTime.String(),
				"average_inblock_time", float64(totalInBlockTime.Milliseconds())/float64(totalBlock),
				"average_crossblock_time", float64(totalCrossBlockTime.Milliseconds())/float64(totalBlock),
				"average_old_inblock_time", float64(totalOldInBlockTime.Milliseconds())/float64(totalBlock),
				"average_fetch_time", float64(totalFetchTime.Milliseconds())/float64(totalBlock),
				"average_process_time", float64(totalProcessTime.Milliseconds())/float64(totalBlock))
			totalBlock = 0
			totalTx = 0
			totalValidTx = 0
			totalCrossBlockDetectionTimes = 0
			totalTxInCrossBlockDetection = 0
			totalValidTxInCrossBlockDetection = 0
			totalSwapTxInCrossBlockDetection = 0
			totalBucketInCrossBlockDetection = 0
			totalTxInBucketInCrossBlockDetection = 0
			totalInBlockTime = 0
			totalCrossBlockTime = 0
			totalOldInBlockTime = 0
			totalFetchTime = 0
			totalProcessTime = 0
		}

		// Update next start slot
		startSlot += uint64(numToFetch)
		// Sleep a while
		// logger.SolLogger.Info("Sleeping for "+config.SOL_FETCH_SLOT_DATA_SHORT_INTERVAL.String(), "next_start", startSlot, "total_blocks_this_run", totalBlock)
		time.Sleep(config.SOL_FETCH_SLOT_DATA_SHORT_INTERVAL)
	}
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

func OldProcessInBlockSandwich(blocks types.Blocks) []*types.InBlockSandwich {
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
				sandwiches := OldFindInBlockSandwiches(b)
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
		AmountThreshold: config.INBLOCK_SANDWICH_AMOUNT_THRESHOLD,
	}

	// timeFind := time.Now()
	finder.Find()

	// logger.SolLogger.Info("Find in-block sandwiches", "slot", b.Slot, "num_txs", len(b.Txs), "num_valid_tx", b.ValidTxCount, "num_bucket", len(finder.buckets), "num_sandwiches", len(finder.Sandwiches), "time_cost", time.Since(timeFind).String())

	return finder.Sandwiches
}

func OldFindInBlockSandwiches(b *types.Block) []*types.InBlockSandwich {
	old_finder := &OldInBlockSandwichFinder{
		Txs:             b.Txs,
		AmountThreshold: config.INBLOCK_SANDWICH_AMOUNT_THRESHOLD,
	}

	// timeFindOld := time.Now()
	old_finder.Find()

	// logger.SolLogger.Info("Find in-block sandwiches (old)", "slot", b.Slot, "num_txs", len(b.Txs), "num_valid_tx", b.ValidTxCount, "num_sandwiches", len(old_finder.Sandwiches), "time_cost", time.Since(timeFindOld).String())
	return old_finder.Sandwiches
}

func ProcessCrossBlockSandwich(blocks types.Blocks) []*types.CrossBlockSandwich {
	if len(blocks) == 0 {
		return make([]*types.CrossBlockSandwich, 0)
	}

	for _, b := range blocks {
		if b != nil {
			crossBlockCache.Put(b)
		}
	}

	all := crossBlockCache.AllBlocks()
	slotToBlock := make(map[uint64]*types.Block, len(all))
	for _, b := range all {
		if b != nil {
			slotToBlock[b.Slot] = b
		}
	}

	// Sort all blocks in cache by slot
	sorted := make(types.Blocks, 0, len(slotToBlock))
	for _, b := range slotToBlock {
		sorted = append(sorted, b)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Slot < sorted[j].Slot
	})

	// Record new slots in current batch
	newSlots := make(map[uint64]struct{}, len(blocks))
	for _, b := range blocks {
		if b != nil {
			newSlots[b.Slot] = struct{}{}
		}
	}

	getLeader := func(slot uint64) string {
		l, err := crossBlockCache.GetSlotLeader(slot)
		if err != nil {
			logger.SolLogger.Warn("GetSlotLeader failed", "slot", slot, "err", err)
			return ""
		}
		return l
	}

	windows := make([]types.Blocks, 0)
	if len(sorted) > 0 {
		runStart := 0
		for i := 1; i <= len(sorted); i++ {
			endRun := i == len(sorted)
			if !endRun {
				prev, curr := sorted[i-1], sorted[i]
				// If slots are not continuous or leader changed, end the run
				if getLeader(prev.Slot) != getLeader(curr.Slot) || curr.Slot != prev.Slot+1 {
					endRun = true
				}
			}
			if !endRun {
				continue
			}

			// Process the run [runStart, i)
			run := sorted[runStart:i] // [runStart, i)
			runStart = i

			// Skip if no new slots in this run
			hasNew := false
			for _, b := range run {
				if _, ok := newSlots[b.Slot]; ok {
					hasNew = true
					break
				}
			}
			if !hasNew {
				continue
			}

			// Build window with left 1 and right 1 blocks if exist
			window := make(types.Blocks, 0, len(run)+2)
			leftSlot := run[0].Slot - 1
			if lb, ok := slotToBlock[leftSlot]; ok {
				window = append(window, lb)
			}
			window = append(window, run...)
			rightSlot := run[len(run)-1].Slot + 1
			if rb, ok := slotToBlock[rightSlot]; ok {
				window = append(window, rb)
			}

			windows = append(windows, window)
		}
	}

	result := make([]*types.CrossBlockSandwich, 0)
	resultSandwichIDSet := make(map[string]bool)
	for _, w := range windows {
		found := FindCrossBlockSandwiches(w)
		if len(found) > 0 {
			// Filter repeated sandwiches according to sandwichID
			for _, s := range found {
				if _, ok := resultSandwichIDSet[s.SandwichID]; !ok {
					result = append(result, s)
					resultSandwichIDSet[s.SandwichID] = true
				}
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Slot != result[j].Slot {
			return result[i].Slot < result[j].Slot
		}
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return result
}

func FindCrossBlockSandwiches(blocks types.Blocks) []*types.CrossBlockSandwich {
	finder := NewCrossBlockSandwichFinder(blocks, config.CROSSBLOCK_SANDWICH_AMOUNT_THRESHOLD)

	finder.Find()

	// Update global stats
	totalCrossBlockDetectionTimes += 1
	totalTxInCrossBlockDetection += uint64(len(finder.Txs))
	totalValidTxInCrossBlockDetection += uint64(finder.ValidTx)
	totalSwapTxInCrossBlockDetection += uint64(finder.ValidSwapTx)
	totalBucketInCrossBlockDetection += uint64(len(finder.buckets))
	totalTxInBucketInCrossBlockDetection += uint64(finder.txInBuckets)

	return finder.Sandwiches
}

func getSlotLeaderFromDB(slot uint64) (string, error) {
	leader, err := ch.QuerySlotLeader(slot)
	if err != nil {
		return "", fmt.Errorf("QuerySlotLeader failed: %w", err)
	}
	return leader, nil
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
		if err := ch.InsertCrossBlockSandwiches(crossBlockSandwiches); err != nil {
			return fmt.Errorf("failed to insert cross-block sandwiches to DB: %w", err)
		}
		logger.SolLogger.Info("Inserted cross-block sandwiches to DB", "num", len(crossBlockSandwiches))

		sandwichTxToInsert := make([]*types.SandwichTx, 0)
		for _, s := range crossBlockSandwiches {
			sandwichTxToInsert = append(sandwichTxToInsert, s.FrontRun...)
			sandwichTxToInsert = append(sandwichTxToInsert, s.Victims...)
			sandwichTxToInsert = append(sandwichTxToInsert, s.BackRun...)
		}
		if err := ch.InsertSandwichTxs(sandwichTxToInsert); err != nil {
			return fmt.Errorf("failed to insert cross-block sandwich txs to DB: %w", err)
		}
		logger.SolLogger.Info("Inserted cross-block sandwich txs to DB", "num", len(sandwichTxToInsert))
	}

	return nil
}

func StoreSlotSandwichStatusToDB(ch db.Database, blks types.Blocks, inBlockSandwiches []*types.InBlockSandwich, crossBlockSandwiches []*types.CrossBlockSandwich) error {
	// DO NOT store sandwich tx count now!
	// Map slot to number of sandwich txs
	// slotToSandwichTxCount := make(map[uint64]uint64)
	// slotToSandwichCount := make(map[uint64]uint64)
	// slotToSandwichVictimCount := make(map[uint64]uint64)
	// In-block sandwiches
	// for _, s := range inBlockSandwiches {
	// 	slotToSandwichTxCount[s.Slot] += uint64(len(s.FrontRun) + len(s.BackRun))
	// 	slotToSandwichCount[s.Slot] += 1
	// 	slotToSandwichVictimCount[s.Slot] += uint64(len(s.Victims))
	// }
	// // Cross-block sandwiches
	// for _, s := range crossBlockSandwiches {
	// 	slotToSandwichTxCount[s.Slot] += uint64(len(s.FrontRun) + len(s.BackRun))
	// 	slotToSandwichCount[s.Slot] += 1
	// 	slotToSandwichVictimCount[s.Slot] += uint64(len(s.Victims))
	// }

	statuses := make([]*types.SlotTxsStatus, 0, len(blks))
	for _, blk := range blks {
		statuses = append(statuses, &types.SlotTxsStatus{
			Slot:            blk.Slot,
			TxFetched:       true,
			TxCount:         uint64(len(blk.Txs)),
			ValidTxCount:    blk.ValidTxCount,
			SandwichFetched: true,
			// SandwichTxCount:         slotToSandwichTxCount[blk.Slot],
			// SandwichCount:           slotToSandwichCount[blk.Slot],
			// SandwichVictimCount:     slotToSandwichVictimCount[blk.Slot],
			SandwichInBundleChecked: false,
		})
	}

	if err := ch.InsertSlotTxs(statuses); err != nil {
		return fmt.Errorf("InsertSlotTxsStatus failed: %w", err)
	}

	return nil
}
