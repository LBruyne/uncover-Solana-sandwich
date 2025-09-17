package jito

import (
	"fmt"
	"time"
	"watcher/config"
	"watcher/db"
	"watcher/logger"
	"watcher/sol"
	"watcher/types"

	MapSet "github.com/deckarep/golang-set/v2"
)

// RunJitoCmd fetches Jito bundles by slot starting from startSlot, and stores them in the database. Also scan sandwichTxs to mark inBundle.
func RunJitoCmd(startSlot uint64) error {
	// Initialize db
	ch := db.NewClickhouse()
	defer ch.Close()

	// First load existing bundles from DB into cache to avoid re-insertion/overlap
	if s, ok, err := ch.QueryLatestBundleSlot(); err != nil {
		return fmt.Errorf("QueryLatestBundleSlot failed: %w", err)
	} else if ok {
		logger.SolLogger.Info("Last bundle in DB", "slot", s)
		if s >= config.MIN_START_SLOT && s >= startSlot {
			startSlot = s + 1
		}
	}
	// Fetch current slot from Solana RPC
	currentSlot, err := sol.GetCurrentSlot()
	if err != nil {
		return fmt.Errorf("GetCurrentSlot failed: %w", err)
	}
	if startSlot > currentSlot {
		logger.SolLogger.Warn("Start slot is greater than current slot, nothing to do", "start", startSlot, "current", currentSlot)
		return nil
	}
	logger.JitoLogger.Info("Starting Jito bundle fetcher", "start_slot", startSlot, "current_slot", currentSlot)

	// Task 1: fetch bundles by slot, from startSlot
	go func(_startSlot uint64) {
		s := _startSlot
		for {
			head, err := sol.GetCurrentSlot()
			if err != nil {
				logger.JitoLogger.Error("GetCurrentSlot failed", "err", err)
				continue
			}
			if s > head {
				logger.JitoLogger.Info("Reached current slot, sleep and retry", "slot", s, "current", head)
				time.Sleep(config.JITO_CHECK_SANDWICH_INTERVAL)
				continue
			}

			bundles, err := GetBundlesBySlot(s)
			if err != nil {
				logger.JitoLogger.Error("GetBundlesBySlot failed", "slot", s, "err", err)
				continue
			}

			// Parse timestamps and filter out invalid bundles
			validBundles := make(types.JitoBundles, 0, len(bundles))
			txCount := uint64(0)
			for _, b := range bundles {
				ts, err := time.Parse(time.RFC3339, b.Timestamp)
				if err != nil {
					logger.JitoLogger.Warn("Failed to parse timestamp, skipping bundle", "bundleId", b.BundleId, "err", err)
					continue
				}
				validBundles = append(validBundles, &types.JitoBundle{
					Slot:              b.Slot,
					BundleId:          b.BundleId,
					Timestamp:         ts,
					Tippers:           b.Tippers,
					LandedTipLamports: b.LandedTipLamports,
					Transactions:      b.TxSignatures,
				})
				txCount += uint64(len(b.TxSignatures))
			}

			// Insert new bundles into DB
			if err := ch.InsertJitoBundles(validBundles); err != nil {
				logger.JitoLogger.Error("InsertJitoBundles failed", "err", err)
			}
			logger.JitoLogger.Info("Inserted bundles", "count", len(validBundles), "slot", s)

			// Update slot_bundle status
			status := types.SlotBundlesStatus{
				Slot:          s,
				BundleFetched: true,
				BundleCount:   uint64(len(validBundles)),
				BundleTxCount: txCount,
			}

			if err := ch.InsertSlotBundles([]*types.SlotBundlesStatus{&status}); err != nil {
				logger.JitoLogger.Error("InsertSlotBundles failed", "slot", s, "err", err)
			} else {
				logger.JitoLogger.Info("Slot bundle status updated", "slot", s, "bundle_count", len(validBundles), "tx_count", txCount)
			}
			s++
		}
	}(startSlot)

	// Task 2: scan sandwich txs to mark inBundle
	for {
		// Find the first (oldest) slot in sandwich_txs, that has already checked sandwich, but not yet checked inBundle and bundles have been fetched.
		slot, err := ch.QueryFirstSlotToCheckInBundle()
		if err != nil {
			logger.JitoLogger.Error("QueryFirstSlotToCheckInBundle failed", "err", err)
			time.Sleep(config.JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL)
			continue
		}
		if slot < config.MIN_START_SLOT {
			logger.JitoLogger.Info("No slot needs sandwich check, sleep and retry", "slot", slot, "interval", config.JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL.String())
			time.Sleep(config.JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL)
			continue
		}
		logger.JitoLogger.Info("Checking sandwich_txs in bundle", "slot", slot)

		// For that slot, query all bundle txs
		bundleTxs, err := ch.QueryBundleTxsBySlot(slot)
		if err != nil {
			logger.JitoLogger.Error("QueryBundleTxsBySlot failed", "slot", slot, "err", err)
			time.Sleep(config.JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL)
			continue
		}
		// For that slot, query all sandwich txs
		swTxs, err := ch.QuerySandwichTxsBySlot(slot)
		if err != nil {
			logger.JitoLogger.Error("QuerySandwichTxsBySlot failed", "slot", slot, "err", err)
			time.Sleep(config.JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL)
			continue
		}

		hit := intersectTxs(bundleTxs, swTxs)
		logger.JitoLogger.Info("Sandwich check", "slot", slot, "bundle_txs", len(bundleTxs), "sandwich_txs", len(swTxs), "hits", len(hit))
		// Mark those sandwich txs as inBundle = true
		if len(hit) > 0 {
			if err := ch.UpdateSandwichTxsInBundle(slot, hit); err != nil {
				logger.JitoLogger.Error("UpdateSandwichTxsInBundle failed", "slot", slot, "err", err, "hits", len(hit))
			} else {
				logger.JitoLogger.Info("Finished marking sandwich_txs", "slot", slot, "hits", len(hit))
			}
		}
		// Finally mark that slot as checked
		if err := ch.UpdateSlotTxsCheckInBundle(slot, true); err != nil {
			logger.JitoLogger.Error("UpdateSlotTxsCheckInBundle failed", "slot", slot, "err", err)
		}
		logger.JitoLogger.Info("Finished checking sandwich_txs in bundle for", "slot", slot, "num_sandwich_txs_in_bundle", len(hit))
	}
}

func intersectTxs(txsA, txsB []string) []string {
	setA := MapSet.NewSet[string]()
	for _, tx := range txsA {
		setA.Add(tx)
	}
	setB := MapSet.NewSet[string]()
	for _, tx := range txsB {
		setB.Add(tx)
	}
	intersection := setA.Intersect(setB)
	return intersection.ToSlice()
}
