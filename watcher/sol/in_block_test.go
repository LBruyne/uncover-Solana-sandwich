package sol

import (
	"fmt"
	"testing"
	"time"

	"watcher/types"
)

var slot uint64

func init() {
	SolonaRpcURL = "http://185.209.179.15:8899"
	slot = 363597000 // Replace a fresh slot id
}

// Pretty print sandwich txs
func ppSandwichTxs(kind string, txs []*types.SandwichTx) {
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
func ppInBlockSandwich(i int, s *types.InBlockSandwich) {
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

func TestFindInBlockSandwiches_Slot(t *testing.T) {
	blk, err := GetBlock(slot)
	if err != nil {
		t.Fatalf("GetBlock(%d) error: %v", slot, err)
	}
	if blk == nil || len(blk.Txs) == 0 {
		t.Fatalf("empty block or no txs at slot %d", slot)
	}
	fmt.Printf("Loaded block: slot=%d height=%d time=%s txs=%d\n\n",
		blk.Slot, blk.BlockHeight, blk.Timestamp.Format(time.RFC3339), len(blk.Txs))

	res := FindInBlockSandwiches(blk)

	fmt.Printf("Detected %d in-block sandwiches in slot=%d\n\n", len(res), slot)
	for i, s := range res {
		ppInBlockSandwich(i, s)
	}

	for _, s := range res {
		if s == nil {
			t.Fatalf("nil sandwich encountered")
		}
		if len(s.FrontRun) == 0 || len(s.BackRun) == 0 || len(s.Victims) == 0 {
			t.Fatalf("invalid sandwich parts: empty front/back/victim")
		}
		for _, f := range s.FrontRun {
			if !(f.FromToken == s.TokenA && f.ToToken == s.TokenB) {
				t.Fatalf("front direction mismatch: got %s->%s, expect %s->%s",
					f.FromToken, f.ToToken, s.TokenA, s.TokenB)
			}
		}
		for _, v := range s.Victims {
			if !(v.FromToken == s.TokenA && v.ToToken == s.TokenB) {
				t.Fatalf("victim direction mismatch: got %s->%s, expect %s->%s",
					v.FromToken, v.ToToken, s.TokenA, s.TokenB)
			}
		}
		for _, b := range s.BackRun {
			if !(b.FromToken == s.TokenB && b.ToToken == s.TokenA) {
				t.Fatalf("back direction mismatch: got %s->%s, expect %s->%s",
					b.FromToken, b.ToToken, s.TokenB, s.TokenA)
			}
		}
	}
}
