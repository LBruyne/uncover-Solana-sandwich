package config

import "time"

const (
	LogPath    = "/home/hins/code/solana-mev/watcher/logs/"
	ConfigPath = "./"
)

// Input config
const (
	MIN_START_SLOT = 330000000
)

const (
	// Averagely 60-80 bundles in a slot
	// A slot is 0.4s
	// Around 20000 bundles in a minute
	JITO_RECENT_FETCH_LIMIT    = 30000
	JITO_RECENT_FETCH_LOWER    = 1000
	JITO_RECENT_FETCH_INTERVAL = 5 * time.Second
)
