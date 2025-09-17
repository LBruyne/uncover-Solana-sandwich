CREATE TABLE IF NOT EXISTS solwich.slot_leaders
(
    `slot` UInt64,
    `leader` String
)
ENGINE = ReplacingMergeTree
PRIMARY KEY slot
ORDER BY slot
SETTINGS index_granularity = 8192
