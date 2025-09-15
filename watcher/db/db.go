package db

import (
	"watcher/types"
)

type Database interface {
	Close() error
	CreateTables() error
	// QueryLastSandwichNoBundle() (uint64, time.Time, error)
	// QueryLastSlotLeader() (uint64, error)

	Exec(query string, args ...any) error
	InsertJitoBundles(bundles types.JitoBundles) error
	InsertSlotLeaders(leaders types.SlotLeaders) error
	InsertInBlockSandwiches(rows []*types.InBlockSandwich) error
	InsertSandwichTxs(sandwichTxs []*types.SandwichTx) error

	// QueryJitoBundles(query string, args ...any) ([]*JitoBundle, error)
	// QueryLastSandwich() (uint64, time.Time, error)

	QueryLatestBundleIds(limit uint) ([]string, error)
	QueryLastSlotLeader() (uint64, error)

	// QueryTxs(query string, args ...any) ([]*Transaction, error)
	// QueryLastTx() (uint64, time.Time, error)
	// QueryEarliesTx() (uint64, time.Time, error)
}
