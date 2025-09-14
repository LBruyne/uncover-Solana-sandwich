package types

import "time"

type SandwichTxTokenInfo struct {
	FromToken  string  `gorm:"column:fromToken; type:varchar(255)" ch:"fromToken"`
	ToToken    string  `gorm:"column:toToken; type:varchar(255)" ch:"toToken"`
	FromAmount float64 `gorm:"column:fromAmount" ch:"fromAmount"`
	ToAmount   float64 `gorm:"column:toAmount" ch:"toAmount"`

	FromTotalAmount float64 `gorm:"column:fromTotalAmount" ch:"fromTotalAmount"` // Only last front-run or back-run tx in a multi-front or multi-back sandwich has the total amount
	ToTotalAmount   float64 `gorm:"column:toTotalAmount" ch:"toTotalAmount"`     // Only last front-run or back-run tx in a multi-front or multi-back sandwich has the total amount

	DiffA float64 `gorm:"column:diffA" ch:"diffA"` // backTx.ToTotal - frontTx.FromTotal
	DiffB float64 `gorm:"column:diffB" ch:"diffB"` // frontTx.ToTotal - backTx.FromTotal
}

type SandwichTx struct {
	Transaction
	SandwichTxTokenInfo
	InBundle bool   `ch:"inbundle"`
	Type     string `ch:"type"` // frontRun, backRun, or victim
}

// Sandwich represents a detected sandwich transaction, including front-run, victim(s) and back-run like A->B, A->B, B->A
type Sandwich struct {
	TokenA            string `ch:"tokenA"`
	TokenB            string `ch:"tokenB"`
	CrossBlock        bool   `ch:"crossBlock"`        // whether the sandwich spans multiple blocks
	Consecutive       bool   `ch:"consecutive"`       // whether the sandwich txs are consecutive in the block, i.e., F_last + 1 == V_first and V_last + 1 == B_first
	FrontConsecutive  bool   `ch:"frontConsecutive"`  // whether the front-run txs are consecutive in the block, i.e., F_1 + 1 == F_2, F_2 + 1 == F_3, ...
	BackConsecutive   bool   `ch:"backConsecutive"`   // whether the back-run txs are consecutive in the block, i.e., B_1 + 1 == B_2, B_2 + 1 == B_3, ...
	VictimConsecutive bool   `ch:"victimConsecutive"` // whether the victim txs are consecutive in the block, i.e., V_1 + 1 == V_2, V_2 + 1 == V_3, ...

	MultiFrontRun bool `ch:"multiFrontRun"` // whether there are multiple front-run txs
	MultiBackRun  bool `ch:"multiBackRun"`  // whether there are multiple back-run txs
	MultiVictim   bool `ch:"multiVictim"`   // whether there are multiple victim txs

	SignerSame bool `ch:"signerSame"` // whether front-run and back-run have the same signer
	OwnerSame  bool `ch:"ownerSame"`  // whether front-run and back-run have the same owner, i.e., the owner of ATA that holds the toToken in front-run and the fromToken in back-run
	ATASame    bool `ch:"ataSame"`    // whether front-run and back-run have the same ATA that holds the toToken in front-run and the fromToken in back-run

	Perfect       bool    `ch:"perfect"`       // whether the sandwich is perfect, i.e., the amount diff of tokeb Bis exactly the same
	RelativeDiffB float64 `ch:"relativeDiffB"` // The relative amount diff = |backTxs.fromTotalAmount - frontTxs.toTotalAmount| / max(frontTxs.toTotalAmount, backTxs.fromTotalAmount).
	ProfitA       float64 `ch:"profitA"`       // The profit of the sandwich = backTx.toToTalAmount - frontTx.fromTotalAmount

	FrontRun []*SandwichTx `ch:"frontRunTx" json:"frontRunTx"`
	BackRun  []*SandwichTx `ch:"backRunTx" json:"backRunTx"`
	Victims  []*SandwichTx `ch:"victims" json:"victims"`
}

// InBlockSandwich is a detected sandwich transaction, that front-run, victim(s) and back-run are all in the same block
type InBlockSandwich struct {
	Sandwich
	Slot      uint64    `gorm:"column:slot" ch:"slot" json:"slot"`
	Timestamp time.Time `gorm:"column:timestamp" ch:"timestamp" json:"timestamp"`
}

type CrossBlockSandwich struct {
	Sandwich
}
