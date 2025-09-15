CREATE TABLE IF NOT EXISTS solwich.sandwich_txs
(
    `sandwichId` String,
    `type` String,           -- "frontRun" / "victim" / "backRun"

    `slot` UInt64,
    `position` Int32,
    `timestamp` DateTime,
    `signature` String,
    `signer` String,
    `inBundle` Bool,
    `AccountKeys` Array(String),
    `Programs` Array(String),

    `fromToken` String,
    `toToken` String,
    `fromAmount` Float64,
    `toAmount` Float64,

    -- Only the last front-run or back-run tx in a multi-front or multi-back sandwich has the total amount
    `fromTotalAmount` Float64,
    `toTotalAmount` Float64,

    -- Only the last back-run txs in a sandwich have the diff
    `diffA` Float64,         -- back.ToTotal - front.FromTotal
    `diffB` Float64          -- front.ToTotal - back.FromTotal
)
ENGINE = MergeTree
ORDER BY (slot, timestamp)
SETTINGS index_granularity = 8192;