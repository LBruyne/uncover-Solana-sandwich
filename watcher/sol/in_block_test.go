package sol

import (
	"fmt"
	"testing"
	"time"

	"watcher/logger"
	"watcher/types"
)

var slot uint64

func init() {
	SolonaRpcURL = "http://185.209.179.15:8899"
	slot = 366883341 // Replace a fresh slot id
	logger.InitLogs("test")
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
		types.PPInBlockSandwich(i+1, s)
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
