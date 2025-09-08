CREATE TABLE solwich.jito_bundles
(
    `bundleId` String,
    `timestamp` DateTime,
    `tippers` Array(String),
    `transactions` Array(String),
    `landedTipLamports` UInt64
)
ENGINE = MergeTree
ORDER BY timestamp
SETTINGS index_granularity = 8192
