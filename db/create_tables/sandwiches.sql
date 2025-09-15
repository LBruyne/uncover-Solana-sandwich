CREATE TABLE IF NOT EXISTS solwich.sandwiches
(
    `sandwichId` String,     -- Hash of frontTx.signature + backTx.signature
    `crossBlock` Bool,
    `slot` UInt64,           
    `timestamp` DateTime, 

    `tokenA` String,
    `tokenB` String,
    `consecutive` Bool,
    `frontConsecutive` Bool,
    `backConsecutive` Bool,
    `victimConsecutive` Bool,

    `signerSame` Bool,
    `ownerSame` Bool,
    `ataSame` Bool,

    `perfect` Bool,
    `relativeDiffB` Float64,
    `profitA` Float64,

    `multiFrontRun` Bool,
    `multiBackRun` Bool,
    `multiVictim` Bool,
    `frontCount` UInt16,
    `backCount` UInt16,
    `victimCount` UInt16
)
ENGINE = MergeTree
ORDER BY (slot, timestamp, sandwichId)
SETTINGS index_granularity = 8192;