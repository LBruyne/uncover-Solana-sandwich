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

	InsertJitoBundles(bundles types.JitoBundles) error
	UpsertSlotBundleStatus(slot uint64, fetched bool, bundleCount, bundleTxCount uint64, checked bool) error
	UpdateSlotBundleStatusSandwichChecked(slot uint64) error
	InsertSlotLeaders(leaders types.SlotLeaders) error
	InsertInBlockSandwiches(rows []*types.InBlockSandwich) error
	InsertSandwichTxs(sandwichTxs []*types.SandwichTx) error
	UpdateSandwichTxsInBundle(slot uint64, txSignatures []string) error

	QueryLatestBundleIds(limit uint) ([]string, error)
	QueryLatestBundleSlot() (uint64, bool, error)
	QueryLastSlotLeader() (uint64, error)
	QueryOldestSlotNeedingSandwichCheck() (uint64, error)
	QueryBundleTxsBySlot(slot uint64) ([]string, error)
	QuerySandwichTxsBySlot(slot uint64) ([]string, error)
}
