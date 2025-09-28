CREATE TABLE IF NOT EXISTS solwich.slot_bundles
(
    `slot` UInt64,
    `bundleFetched` Bool,
    `bundleCount` UInt64,
    `bundleTxCount` UInt64
)
ENGINE = ReplacingMergeTree
PRIMARY KEY slot
ORDER BY slot
SETTINGS index_granularity = 8192
