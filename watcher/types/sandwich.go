package types

import (
	"fmt"
	"time"
)

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

// Pretty print sandwich txs
func ppSandwichTxs(kind string, txs []*SandwichTx) {
	fmt.Printf("  %s (%d):\n", kind, len(txs))
	for i, stx := range txs {
		pos := stx.Position
		sig := stx.Signature
		signer := stx.Signer
		ti := stx.SandwichTxTokenInfo
		fmt.Printf("    [%d] pos=%d signer=%s sig=%s\n", i, pos, signer, sig)
		fmt.Printf("         from=%s amt=%.9f  to=%s amt=%.9f\n",
			ti.FromToken, ti.FromAmount, ti.ToToken, ti.ToAmount)
		if ti.FromTotalAmount != 0 || ti.ToTotalAmount != 0 || ti.DiffA != 0 || ti.DiffB != 0 {
			fmt.Printf("         totals: fromTotal=%.9f  toTotal=%.9f  diffA=%.9f  diffB=%.9f\n",
				ti.FromTotalAmount, ti.ToTotalAmount, ti.DiffA, ti.DiffB)
		}
	}
}

// Pretty print an in-block sandwich
func PPInBlockSandwich(i int, s *InBlockSandwich) {
	fmt.Printf("==== Sandwich #%d ====\n", i)
	fmt.Printf("slot=%d time=%s\n", s.Slot, s.Timestamp.Format(time.RFC3339))
	fmt.Printf("pair: A=%s  B=%s\n", s.TokenA, s.TokenB)
	fmt.Printf("flags: CrossBlock=%v Consecutive=%v FrontConsec=%v BackConsec=%v VictimConsec=%v\n",
		s.CrossBlock, s.Consecutive, s.FrontConsecutive, s.BackConsecutive, s.VictimConsecutive)
	fmt.Printf("multi: Front=%v Back=%v Victim=%v  SignerSame=%v OwnerSame=%v ATASame=%v\n",
		s.MultiFrontRun, s.MultiBackRun, s.MultiVictim, s.SignerSame, s.OwnerSame, s.ATASame)
	fmt.Printf("quality: Perfect=%v RelativeDiffB=%.9f ProfitA=%.9f\n",
		s.Perfect, s.RelativeDiffB, s.ProfitA)

	ppSandwichTxs("FrontRun", s.FrontRun)
	ppSandwichTxs("Victims", s.Victims)
	ppSandwichTxs("BackRun", s.BackRun)
	fmt.Println()
}

func summarizeCrossBlockSpan(s *CrossBlockSandwich) (minSlot, maxSlot uint64, minTime, maxTime time.Time) {
	minSlot = ^uint64(0) // max uint64
	maxSlot = 0
	minTime = time.Unix(1<<62, 0) // 很大的过去时间
	maxTime = time.Unix(0, 0)

	collect := func(txs []*SandwichTx) {
		for _, t := range txs {
			if t == nil {
				continue
			}
			if t.Slot < minSlot {
				minSlot = t.Slot
			}
			if t.Slot > maxSlot {
				maxSlot = t.Slot
			}
			if !t.Timestamp.IsZero() {
				if minTime.IsZero() || t.Timestamp.Before(minTime) {
					minTime = t.Timestamp
				}
				if t.Timestamp.After(maxTime) {
					maxTime = t.Timestamp
				}
			}
		}
	}

	collect(s.FrontRun)
	collect(s.Victims)
	collect(s.BackRun)
	return
}

// Pretty print a cross-block sandwich
func PPCrossBlockSandwich(i int, s *CrossBlockSandwich) {
	fmt.Printf("==== CrossBlock Sandwich #%d ====\n", i)
	minSlot, maxSlot, minTime, maxTime := summarizeCrossBlockSpan(s)
	if minSlot <= maxSlot {
		if !minTime.IsZero() && !maxTime.IsZero() {
			fmt.Printf("slots=[%d..%d] time=[%s .. %s]\n",
				minSlot, maxSlot, minTime.Format(time.RFC3339), maxTime.Format(time.RFC3339))
		} else {
			fmt.Printf("slots=[%d..%d]\n", minSlot, maxSlot)
		}
	}
	fmt.Printf("pair: A=%s  B=%s\n", s.TokenA, s.TokenB)
	fmt.Printf("flags: CrossBlock=%v FrontConsec=%v BackConsec=%v VictimConsec=%v\n",
		s.CrossBlock, s.FrontConsecutive, s.BackConsecutive, s.VictimConsecutive)
	fmt.Printf("multi: Front=%v Back=%v Victim=%v  SignerSame=%v OwnerSame=%v ATASame=%v\n",
		s.MultiFrontRun, s.MultiBackRun, s.MultiVictim, s.SignerSame, s.OwnerSame, s.ATASame)
	fmt.Printf("quality: Perfect=%v RelativeDiffB=%.9f ProfitA=%.9f\n",
		s.Perfect, s.RelativeDiffB, s.ProfitA)

	ppSandwichTxs("FrontRun", s.FrontRun)
	ppSandwichTxs("Victims", s.Victims)
	ppSandwichTxs("BackRun", s.BackRun)
	fmt.Println()
}
