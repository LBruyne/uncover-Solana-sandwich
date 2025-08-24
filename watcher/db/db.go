package db

import (
	"watcher/types"
)

type Database interface {
	Close() error
	CreateTables() error
	// QueryLastSandwichNoBundle() (uint64, time.Time, error)
	// InsertSlotLeaders(slotLeaders []*SlotLeader) error
	// QueryLastSlotLeader() (uint64, error)

	Exec(query string, args ...any) error
	InsertJitoBundles(bundles types.JitoBundles) error
	// InsertJitoSandwiches(sandwiches []*JitoSandwich) error
	// InsertSandwiches(sandwiches []*Sandwich) error
	// InsertTxs(txs []*Transaction) error
	// QueryJitoBundles(query string, args ...any) ([]*JitoBundle, error)
	// QueryLastSandwich() (uint64, time.Time, error)

	QueryLatestBundleIds(limit uint) ([]string, error)

	// QueryTxs(query string, args ...any) ([]*Transaction, error)
	// QueryLastTx() (uint64, time.Time, error)
	// QueryEarliesTx() (uint64, time.Time, error)
}
