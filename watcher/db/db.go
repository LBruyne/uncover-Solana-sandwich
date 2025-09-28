package db

import (
	"watcher/types"
)

type Database interface {
	Close() error
	EnsureDatabaseExists() error
	CreateTables() error
	DropTables() error

	Exec(query string, args ...any) error

	// jito_bundles
	InsertJitoBundles(bundles types.JitoBundles) error
	QueryLatestBundleIds(limit uint) ([]string, error)
	QueryBundleTxsBySlot(slot uint64) ([]string, error)

	// slot_bundles
	InsertSlotBundles(statuses []*types.SlotBundlesStatus) error
	QuerySlotBundleBySlot(slot uint64) (uint64, error)
	QueryEarliestAndLatestBundleSlot() (uint64, uint64, bool, error)

	// slot_txs
	InsertSlotTxs(statuses []*types.SlotTxsStatus) error
	UpdateSlotTxsCheckInBundle(slot uint64, check bool) error

	// slot_leaders
	InsertSlotLeaders(leaders types.SlotLeaders) error
	QueryLastSlotLeader() (uint64, error)

	// sandwiches
	InsertInBlockSandwiches(rows []*types.InBlockSandwich) error
	InsertCrossBlockSandwiches(rows []*types.CrossBlockSandwich) error

	// sandwich_txs
	InsertSandwichTxs(sandwichTxs []*types.SandwichTx) error
	UpdateSandwichTxsInBundle(slot uint64, txSignatures []string) error
	QuerySandwichTxsBySlot(slot uint64) ([]string, error)

	// others
	QueryFirstSlotToCheckInBundle() (uint64, error) // First slot in slot_txs where slot_txs.SandwichInBundleChecked = false, slot_txs.SandwichFetched = slot_txs.txFetched = true, and also exists in slot_bundles that slot_bundles.bundleFetched = true
}
