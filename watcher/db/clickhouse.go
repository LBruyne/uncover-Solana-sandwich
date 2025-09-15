package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"
	"watcher/logger"
	"watcher/types"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

type ClickhouseDB struct {
	conn driver.Conn
}

func NewClickhouse() Database {
	opts := &clickhouse.Options{
		Addr: []string{viper.GetString("CLICKHOUSE_ADDR")},
		Auth: clickhouse.Auth{
			Database: viper.GetString("CLICKHOUSE_DATABASE"),
			Username: viper.GetString("CLICKHOUSE_USERNAME"),
			Password: viper.GetString("CLICKHOUSE_PASSWORD"),
		},
		DialTimeout:  5 * time.Second,
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		MaxOpenConns: 10,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		slog.Error("Failed to connect to ClickHouse", "error", err)
	}

	db := &ClickhouseDB{conn: conn}
	// if err := db.CreateTables(); err != nil {
	// 	panic(fmt.Sprintf("failed to create tables: %v", err))
	// }
	return db
}

// Database interface implementation
func (d *ClickhouseDB) Close() error {
	return d.conn.Close()
}

func (d *ClickhouseDB) EnsureDatabaseExists() error {
	query := `CREATE DATABASE IF NOT EXISTS solwich`
	if err := d.conn.Exec(context.Background(), query); err != nil {
		return fmt.Errorf("failed to ensure database exists: %w", err)
	}
	logger.GlobalLogger.Info("Database ensured to exist", "database", "solwich")
	return nil
}

func (d *ClickhouseDB) CreateTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS solwich.jito_bundles
		(
			bundleId String,
			timestamp DateTime,
			tippers Array(String),
			transactions Array(String),
			landedTipLamports UInt64
		)
		ENGINE = MergeTree
		ORDER BY timestamp
		SETTINGS index_granularity = 8192`,

		`CREATE TABLE IF NOT EXISTS solwich.slot_leaders
		(
			slot UInt64,
			leader String
		)
		ENGINE = ReplacingMergeTree
		PRIMARY KEY slot
		ORDER BY slot
		SETTINGS index_granularity = 8192`,

		`CREATE TABLE IF NOT EXISTS solwich.sandwiches
		(
			sandwichId String,
			crossBlock Bool,
			slot UInt64,
			timestamp DateTime,

			tokenA String,
			tokenB String,
			consecutive Bool,
			frontConsecutive Bool,
			backConsecutive Bool,
			victimConsecutive Bool,

			signerSame Bool,
			ownerSame Bool,
			ataSame Bool,

			perfect Bool,
			relativeDiffB Float64,
			profitA Float64,

			multiFrontRun Bool,
			multiBackRun Bool,
			multiVictim Bool,
			frontCount UInt16,
			backCount UInt16,
			victimCount UInt16
		)
		ENGINE = MergeTree
		ORDER BY (slot, timestamp, sandwichId)
		SETTINGS index_granularity = 8192`,

		`CREATE TABLE IF NOT EXISTS solwich.sandwich_txs
		(
			sandwichId String,
			type String,

			slot UInt64,
			position Int32,
			timestamp DateTime,
			signature String,
			signer String,
			inBundle Bool,
			accountKeys Array(String),
			programs Array(String),

			fromToken String,
			toToken String,
			fromAmount Float64,
			toAmount Float64,

			fromTotalAmount Float64,
			toTotalAmount Float64,

			diffA Float64,
			diffB Float64
		)
		ENGINE = MergeTree
		ORDER BY (slot, timestamp)
		SETTINGS index_granularity = 8192`,
	}

	for _, q := range queries {
		if err := d.conn.Exec(context.Background(), q); err != nil {
			return err
		}
		logger.GlobalLogger.Info("Check or create table in DB", "query", q)
	}
	return nil
}

func (d *ClickhouseDB) Exec(query string, args ...any) error {
	if err := d.conn.Exec(context.Background(), query, args...); err != nil {
		return err
	}
	return nil
}

func (d *ClickhouseDB) InsertJitoBundles(bundles types.JitoBundles) error {
	batch, err := d.conn.PrepareBatch(context.Background(), "INSERT INTO jito_bundles")
	if err != nil {
		return err
	}
	for _, bundle := range bundles {
		if err := batch.AppendStruct(bundle); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (d *ClickhouseDB) InsertSlotLeaders(leaders types.SlotLeaders) error {
	batch, err := d.conn.PrepareBatch(context.Background(), "INSERT INTO slot_leaders")
	if err != nil {
		return err
	}
	for _, leader := range leaders {
		if err := batch.AppendStruct(leader); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (d *ClickhouseDB) InsertInBlockSandwiches(rows []*types.InBlockSandwich) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(context.Background(), "INSERT INTO solwich.sandwiches")
	if err != nil {
		return err
	}
	for _, s := range rows {
		if err := batch.AppendStruct(&s); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (d *ClickhouseDB) InsertSandwichTxs(sandwichTxs []*types.SandwichTx) error {
	if len(sandwichTxs) == 0 {
		return nil
	}
	batch, err := d.conn.PrepareBatch(context.Background(), "INSERT INTO solwich.sandwich_txs")
	if err != nil {
		return err
	}

	for _, tx := range sandwichTxs {
		if err := batch.AppendStruct(&tx); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (d *ClickhouseDB) QueryLatestBundleIds(limit uint) ([]string, error) {
	rows, err := d.conn.Query(context.Background(),
		fmt.Sprintf(`SELECT bundleId FROM jito_bundles ORDER BY timestamp DESC LIMIT %d`, limit))
	if err != nil {
		return nil, fmt.Errorf("query latest bundleIds failed: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan bundleId failed: %w", err)
		}
		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return ids, nil
}

func (d *ClickhouseDB) QueryLastSlotLeader() (uint64, error) {
	row := d.conn.QueryRow(context.Background(), "SELECT MAX(slot) from slot_leaders")
	var slot uint64
	if err := row.Scan(&slot); err != nil {
		return 0, fmt.Errorf("query last slot leader failed: %w", err)
	}
	return slot, nil
}

// func (c *ClickhouseDB) QueryJitoBundles(query string, args ...any) ([]*JitoBundle, error) {
// 	rows, err := c.conn.Query(query, args...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var bundles []*JitoBundle
// 	for rows.Next() {
// 		var b JitoBundle
// 		if err := rows.Scan(&b.Slot, &b.Proposer, &b.Hash, &b.BlockTime); err != nil {
// 			return nil, err
// 		}
// 		bundles = append(bundles, &b)
// 	}
// 	return bundles, nil
// }

// func (c *ClickhouseDB) QueryLastSandwich() (uint64, time.Time, error) {
// 	row := c.conn.QueryRow("SELECT slot, block_time FROM sandwiches ORDER BY slot DESC LIMIT 1")
// 	var slot uint64
// 	var t time.Time
// 	if err := row.Scan(&slot, &t); err != nil {
// 		return 0, time.Time{}, err
// 	}
// 	return slot, t, nil
// }

// func (c *ClickhouseDB) QueryLastTx() (uint64, time.Time, error) {
// 	row := c.conn.QueryRow("SELECT slot, block_time FROM txs ORDER BY slot DESC LIMIT 1")
// 	var slot uint64
// 	var t time.Time
// 	if err := row.Scan(&slot, &t); err != nil {
// 		return 0, time.Time{}, err
// 	}
// 	return slot, t, nil
// }

// func (c *ClickhouseDB) QueryEarliesTx() (uint64, time.Time, error) {
// 	row := c.conn.QueryRow("SELECT slot, block_time FROM txs ORDER BY slot ASC LIMIT 1")
// 	var slot uint64
// 	var t time.Time
// 	if err := row.Scan(&slot, &t); err != nil {
// 		return 0, time.Time{}, err
// 	}
// 	return slot, t, nil
// }

// // Placeholder implementations
// func (c *ClickhouseDB) InsertSandwichNoBundle(sandwiches []*SandwichNoBundle) error { return nil }
// func (c *ClickhouseDB) QueryLastSandwichNoBundle() (uint64, time.Time, error) {
// 	return 0, time.Time{}, nil
// }
// func (c *ClickhouseDB) InsertSlotLeaders(slotLeaders []*SlotLeader) error { return nil }
// func (c *ClickhouseDB) QueryLastSlotLeader() (uint64, error)              { return 0, nil }
