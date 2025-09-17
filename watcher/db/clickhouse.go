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
			slot UInt64,
			timestamp DateTime,
			tippers Array(String),
			transactions Array(String),
			landedTipLamports UInt64
		)
		ENGINE = MergeTree
		ORDER BY timestamp
		SETTINGS index_granularity = 8192`,

		`CREATE TABLE IF NOT EXISTS solwich.slot_bundle
		(
			slot UInt64,
			bundleFetched Bool,
			bundleCount UInt64,
			bundleTxCount UInt64,
			SandwichInBundleChecked Bool
		)
		ENGINE = ReplacingMergeTree
		PRIMARY KEY slot
		ORDER BY slot
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

func (d *ClickhouseDB) DropTables() error {
	var dbName string
	if err := d.conn.QueryRow(context.Background(), "SELECT currentDatabase()").Scan(&dbName); err != nil {
		return fmt.Errorf("failed to get current database: %w", err)
	}

	rows, err := d.conn.Query(context.Background(),
		fmt.Sprintf("SHOW TABLES FROM %s", dbName))
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, t)
	}

	for _, t := range tables {
		q := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", dbName, t)
		if err := d.conn.Exec(context.Background(), q); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", t, err)
		}
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

func (d *ClickhouseDB) UpsertSlotBundleStatus(slot uint64, fetched bool, bundleCount, bundleTxCount uint64, checked bool) error {
	q := `INSERT INTO solwich.slot_bundle (slot, bundleFetched, bundleCount, bundleTxCount, SandwichInBundleChecked) VALUES (?, ?, ?, ?, ?)`
	return d.conn.Exec(context.Background(), q, slot, fetched, bundleCount, bundleTxCount, checked)
}

func (d *ClickhouseDB) UpsertSandwichFetched(slot uint64) error {
	ctx := context.Background()
	if err := d.conn.Exec(ctx, "SET mutations_sync = 1"); err != nil {
		return err
	}

	if err := d.conn.Exec(ctx,
		"ALTER TABLE solwich.slot_bundle UPDATE SandwichFetched = 1 WHERE slot = ?",
		slot,
	); err != nil {
		return err
	}

	const insertIfNotExists = `
	INSERT INTO solwich.slot_bundle
		(slot, SandwichFetched, bundleFetched, bundleCount, bundleTxCount, SandwichInBundleChecked)
	SELECT
		?,    /* slot */
		1,    /* SandwichFetched */
		0,    /* bundleFetched */
		0,    /* bundleCount */
		0,    /* bundleTxCount */
		0     /* SandwichInBundleChecked */
	FROM system.one
	WHERE NOT EXISTS (SELECT 1 FROM solwich.slot_bundle WHERE slot = ?)
	`
	return d.conn.Exec(ctx, insertIfNotExists, slot, slot)
}

func (d *ClickhouseDB) UpdateSlotBundleStatusSandwichChecked(slot uint64) error {
	if err := d.conn.Exec(context.Background(), "SET mutations_sync = 1"); err != nil {
		return err
	}
	q := "ALTER TABLE solwich.slot_bundle UPDATE SandwichInBundleChecked = 1 WHERE slot = ?"
	return d.conn.Exec(context.Background(), q, slot)
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
		if err := batch.AppendStruct(s); err != nil {
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
		if err := batch.AppendStruct(tx); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (d *ClickhouseDB) UpdateSandwichTxsInBundle(slot uint64, txsInBundle []string) error {
	if len(txsInBundle) == 0 {
		return nil
	}
	params := make([]any, 0, len(txsInBundle)+1)
	params = append(params, slot)
	inList := "?"
	for i := 1; i < len(txsInBundle); i++ {
		inList += ", ?"
	}
	for _, s := range txsInBundle {
		params = append(params, s)
	}

	q := fmt.Sprintf(`ALTER TABLE solwich.sandwich_txs UPDATE inBundle = 1 WHERE slot = ? AND signature IN (%s)`, inList)
	return d.conn.Exec(context.Background(), q, params...)
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

func (d *ClickhouseDB) QueryLatestBundleSlot() (uint64, bool, error) {
	row := d.conn.QueryRow(context.Background(),
		`SELECT max(slot) FROM solwich.slot_bundle WHERE bundleFetched = 1`)
	var slot *uint64
	if err := row.Scan(&slot); err != nil {
		return 0, false, fmt.Errorf("QueryLatestBundleSlot scan failed: %w", err)
	}
	if slot == nil {
		return 0, false, nil
	}
	return *slot, true, nil
}

func (d *ClickhouseDB) QueryLastSlotLeader() (uint64, error) {
	row := d.conn.QueryRow(context.Background(), "SELECT MAX(slot) from slot_leaders")
	var slot uint64
	if err := row.Scan(&slot); err != nil {
		return 0, fmt.Errorf("query last slot leader failed: %w", err)
	}
	return slot, nil
}

func (d *ClickhouseDB) QueryOldestSlotNeedingSandwichCheck() (uint64, error) {
	// First find the max slot of all recorded sandwich txs (maybe empty)
	var maxSandwichSlot uint64
	if err := d.conn.QueryRow(context.Background(),
		`SELECT ifNull(max(slot), toUInt64(0)) FROM solwich.sandwich_txs`).Scan(&maxSandwichSlot); err != nil {
		return 0, err
	}
	if maxSandwichSlot == 0 {
		return 0, nil
	}
	// Find the oldest slot <= maxSandwichSlot that has fetched bundles but not yet checked for sandwiches
	row := d.conn.QueryRow(context.Background(),
		`SELECT ifNull(min(slot), toUInt64(0)) 
         FROM solwich.slot_bundle 
         WHERE bundleFetched = 1 AND SandwichInBundleChecked = 0 AND slot <= ?`, maxSandwichSlot)
	var slot uint64
	if err := row.Scan(&slot); err != nil {
		return 0, err
	}
	if slot == 0 {
		return 0, nil
	}
	return slot, nil
}

func (d *ClickhouseDB) QueryBundleTxsBySlot(slot uint64) ([]string, error) {
	rows, err := d.conn.Query(context.Background(), `SELECT DISTINCT arrayJoin(transactions) FROM solwich.jito_bundles WHERE slot = ?`, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	txs := make([]string, 0)
	for rows.Next() {
		var tx string
		if err := rows.Scan(&tx); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}

func (d *ClickhouseDB) QuerySandwichTxsBySlot(slot uint64) ([]string, error) {
	rows, err := d.conn.Query(context.Background(),
		`SELECT DISTINCT signature FROM solwich.sandwich_txs WHERE slot = ?`, slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	txs := make([]string, 0)
	for rows.Next() {
		var tx string
		if err := rows.Scan(&tx); err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, rows.Err()
}
