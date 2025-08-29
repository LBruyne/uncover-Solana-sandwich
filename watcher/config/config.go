package config

import "time"

const (
	LogPath    = "/home/hins/code/solana-mev/watcher/logs/"
	ConfigPath = "./"
)

// Input config
const (
	MIN_START_SLOT = 340000000
)

const (
	// Averagely 60-80 bundles in a slot
	// A slot is 0.4s
	// Around 20000 bundles in a minute
	JITO_RECENT_FETCH_LIMIT    = 30000
	JITO_RECENT_FETCH_LOWER    = 1000
	JITO_RECENT_FETCH_INTERVAL = 5 * time.Second

	SOL_FETCH_SLOT_LIMIT          = 5000
	SOL_FETCH_SLOT_LOWER          = 2000
	SOL_FETCH_SLOT_MAX_GAP        = 4000000 // the API can preserve data ~0.5 month ago
	SOL_FETCH_SLOT_SHORT_INTERVAL = 400 * time.Millisecond
	SOL_FETCH_SLOT_LONG_INTERVAL  = 1000 * time.Second
)
