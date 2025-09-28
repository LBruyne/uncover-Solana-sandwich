package config

import "time"

// Path config
const (
	LogPath    = "./logs/"
	ConfigPath = "./"
)

// Input config
const (
	MIN_START_SLOT = 350000000
)

// Network config
const (
	DefaultRetryTimes    = 3
	DefaultRetryInterval = 50 * time.Millisecond
	DefaultTimeout       = 20 * time.Second
)

// Fetch config
const (
	// Jito recent bundles
	// Averagely 60-80 bundles in a slot
	// A slot is 0.4s
	// Around 20000 bundles in a minute
	JITO_RECENT_FETCH_LIMIT    = 30000
	JITO_RECENT_FETCH_LOWER    = 1000
	JITO_RECENT_FETCH_INTERVAL = 5 * time.Second

	// Jito bundles by slot
	JITO_CHECK_SANDWICH_INTERVAL = 5 * time.Second

	SOL_FETCH_SLOT_LEADER_MAX_GAP        = 4000000 // the API can preserve slot-leader data ~0.5 month ago
	SOL_FETCH_SLOT_LEADER_LIMIT          = 5000
	SOL_FETCH_SLOT_LEADER_LOWER          = 2000
	SOL_FETCH_SLOT_LEADER_SHORT_INTERVAL = 400 * time.Millisecond
	SOL_FETCH_SLOT_LEADER_LONG_INTERVAL  = 1000 * time.Second

	SOL_FETCH_SLOT_DATA_MAX_GAP        = 10000 // the API can preserve block data ~3 hours ago
	SOL_FETCH_SLOT_DATA_SLOT_NUM       = 8     // number of slots to fetch each time
	SOL_FETCH_SLOT_DATA_PARALLEL_NUM   = 8     // number of parallel requests
	SOL_FETCH_SLOT_DATA_RETRYS         = 3     // number of retries on failure
	SOL_FETCH_SLOT_DATA_LONG_INTERVAL  = 400 * time.Millisecond * SOL_FETCH_SLOT_DATA_SLOT_NUM
	SOL_FETCH_SLOT_DATA_SHORT_INTERVAL = 400 * time.Millisecond
)

// Detection config
const (
	SOL_PROCESS_IN_BLOCK_SANDWICH_PARALLEL_NUM = 8 // number of parallel processing in-block sandwiches

	JITO_MARK_IN_BUNDLE_SANDWICH_TX_INTERVAL = 10 * time.Second // interval to mark sandwich txs in bundle

	SANDWICH_AMOUNT_THRESHOLD            = uint(10) // relative threshold between front-run/back-run
	CROSSBLOCK_SANDWICH_AMOUNT_THRESHOLD = uint(5)
	SANDWICH_AMOUNT_SOL_TOLERANCE        = 0.1

	SANDWICH_BACKRUN_MAX_GAP  = 10 // How long can two back-run txs be apart
	SANDWICH_FRONTRUN_MAX_GAP = 10 // How long can two front-run txs be apart

	CROSS_BLOCK_SANDWICH_MAX_SLOT_GAP             = 10
	SOL_PROCESS_CROSS_BLOCK_SANDWICH_PARALLEL_NUM = 8
	SOL_PROCESS_CROSS_BLOCK_BUCKETS_PARALLEL_NUM  = 8
)
