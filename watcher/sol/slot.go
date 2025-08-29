package sol

import (
	"fmt"
	"time"
	"watcher/config"
	"watcher/db"
	"watcher/logger"
)

func RunSlotCmd(startSlot uint64) error {
	// Initialize db
	ch := db.NewClickhouse()
	defer ch.Close()

	// Fetch last slot leaders recorded in DB
	lastSlotInDB, err := ch.QueryLastSlotLeader()
	if err != nil {
		return fmt.Errorf("failed to query last slot leader: %w", err)
	}
	logger.SolLogger.Info("Last slot leader in DB", "slot", lastSlotInDB)
	// Fetch current slot from Solana RPC
	currentSlotFromRpc, err := GetCurrentSlot()
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}
	logger.SolLogger.Info("Current slot from Solana RPC", "slot", currentSlotFromRpc)
	logger.SolLogger.Info("Fetch slot leaders", "startSlotFromInput", startSlot, "lastSlotInDB", lastSlotInDB, "currentSlotFromRpc", currentSlotFromRpc)
	// Use startSlot, lastSlot, currentSlot to determine the starting point:
	// 1. if startSlot < lastSlotInDB, set startSlot = lastSlotInDB + 1
	startSlot = max(startSlot, lastSlotInDB+1)
	if startSlot > currentSlotFromRpc {
		logger.SolLogger.Warn("Start slot is greater than current slot, nothing to do", "startSlot", startSlot, "currentSlotFromRpc", currentSlotFromRpc)
		return nil
	}
	// 2. if currentSlot - startSlot < max gap, fetch start from startSlot,
	// otherwise, jump to currentSlot directly
	if startSlot+config.SOL_FETCH_SLOT_MAX_GAP > currentSlotFromRpc {
		// Sync from startSlot
		logger.SolLogger.Info("Syncing slot leaders", "startSlot", startSlot)
		for {
			currentSlotFromRpc, err := GetCurrentSlot()
			if err != nil {
				logger.SolLogger.Error("Failed to get current slot", "err", err)
				continue
			}

			var limit uint64
			if startSlot+config.SOL_FETCH_SLOT_LIMIT >= currentSlotFromRpc {
				limit = currentSlotFromRpc - startSlot + 1
				logger.SolLogger.Info("Fetch slot leaders (reaching current slot),", "start", startSlot, "current", currentSlotFromRpc, "limit", limit)
				leaders, err := GetSlotLeaders(startSlot, limit)
				if err != nil {
					logger.SolLogger.Error("Failed to get slot leaders", "start", startSlot, "limit", limit, "err", err)
					continue
				}
				err = ch.InsertSlotLeaders(leaders)
				if err != nil {
					logger.SolLogger.Error("Failed to insert slot leaders", "err", err)
					continue
				} else {
					logger.SolLogger.Info("Inserted slot leaders", "count", len(leaders), "start", startSlot, "limit", limit)
				}
				startSlot += uint64(len(leaders))
				break
			}

			limit = config.SOL_FETCH_SLOT_LIMIT
			logger.SolLogger.Info("Fetch slot leaders", "start", startSlot, "current", currentSlotFromRpc, "limit", limit)
			leaders, err := GetSlotLeaders(startSlot, limit)
			if err != nil {
				logger.SolLogger.Error("Failed to get slot leaders", "start", startSlot, "limit", limit, "err", err)
				continue
			}
			err = ch.InsertSlotLeaders(leaders)
			if err != nil {
				logger.SolLogger.Error("Failed to insert slot leaders", "err", err)
				continue
			} else {
				logger.SolLogger.Info("Inserted slot leaders", "count", len(leaders), "start", startSlot, "limit", limit)
			}
			startSlot += uint64(len(leaders))
			// Sleep a while to avoid hitting rate limit
			time.Sleep(config.SOL_FETCH_SLOT_SHORT_INTERVAL)
		}
	} else {
		startSlot = currentSlotFromRpc
		logger.SolLogger.Info("Start slot too far behind current slot, start from current slot", "startSlot", startSlot)
	}

	// Sync from currentSlot
	logger.SolLogger.Info("Syncing slot leaders from RPC current slot", "currentSlot", currentSlotFromRpc)
	for {
		currentSlot, err := GetCurrentSlot()
		if err != nil {
			logger.SolLogger.Error("Failed to get current slot", "err", err)
			continue
		}

		if currentSlot-startSlot < config.SOL_FETCH_SLOT_LOWER {
			logger.SolLogger.Info("Not enough new slots, sleep and retry", "startSlot", startSlot, "currentSlot", currentSlot)
			time.Sleep(config.SOL_FETCH_SLOT_LONG_INTERVAL)
			continue
		}

		limit := currentSlot - startSlot + 1
		// Limit should not exceed SOL_FETCH_SLOT_LIMIT
		if limit > config.SOL_FETCH_SLOT_LIMIT {
			logger.SolLogger.Warn("Slot gap too large, capping limit to SOL_FETCH_SLOT_LIMIT", "calculatedLimit", limit, "cappedLimit", config.SOL_FETCH_SLOT_LIMIT)
			limit = config.SOL_FETCH_SLOT_LIMIT
		}

		logger.SolLogger.Info("Fetch slot leaders", "start", startSlot, "current", currentSlot, "limit", limit)
		leaders, err := GetSlotLeaders(startSlot, limit)
		if err != nil {
			logger.SolLogger.Error("Failed to get slot leaders", "start", startSlot, "limit", limit, "err", err)
			continue
		}
		err = ch.InsertSlotLeaders(leaders)
		if err != nil {
			logger.SolLogger.Error("Failed to insert slot leaders", "err", err)
			continue
		} else {
			logger.SolLogger.Info("Inserted slot leaders", "count", len(leaders), "start", startSlot, "limit", limit)
		}
		startSlot += uint64(len(leaders))
		// Sleep a while
		time.Sleep(config.SOL_FETCH_SLOT_LONG_INTERVAL)
	}
}
