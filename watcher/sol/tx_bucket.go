package sol

import (
	"sort"
	"watcher/types"
)

// PoolKey is used to group transactions that interact with the same pool and token pair
type PoolKey struct {
	PoolAddress  string // Suppose a sandwich consists of front-run A->B, victim(s) A->B, and back-run B->A. A pool address must have two sides of amount change about A and B.
	IncomeToken  string // From pool's perspective, in frontTx, A is incomeToken, delta > 0; in backTx, B is incomeToken, delta < 0
	ExpenseToken string // From pool's perspective, in frontTx, b is expenseToken, delta < 0; in backTx, A is expenseToken, delta > 0
}

type PoolEntry struct {
	TxIdx    int // Index of the transaction in the original txs slice (for sandwich detection)
	Slot     uint64
	Position int
	Signer   string

	// Related pool and token info
	PoolAddress  string
	IncomeToken  string
	ExpenseToken string
	IncomeAmt    float64
	ExpenseAmt   float64
}

func buildTxBuckets(txs types.Transactions, crossBlock bool) map[PoolKey][]PoolEntry {
	buckets := make(map[PoolKey][]PoolEntry)
	for idx, tx := range txs {
		if tx == nil || tx.IsFailed || tx.IsVote {
			continue
		}
		if tx.RelatedPools.Cardinality() == 0 || tx.RelatedTokens.Cardinality() < 2 {
			continue
		}
		for pool, poolAmt := range tx.RelatedPoolsInfo {
			// Validate poolAmount
			if poolAmt.IncomeToken == "" || poolAmt.ExpenseToken == "" {
				continue
			}
			// Pool address must have two sides of amount change
			if !(poolAmt.IncomeAmt > 0 && poolAmt.ExpenseAmt < 0) {
				continue
			}
			key := PoolKey{PoolAddress: pool, IncomeToken: poolAmt.IncomeToken, ExpenseToken: poolAmt.ExpenseToken}
			entry := PoolEntry{
				TxIdx:        idx,
				Slot:         tx.Slot,
				Position:     tx.Position,
				Signer:       tx.Signer,
				PoolAddress:  pool,
				IncomeToken:  poolAmt.IncomeToken,
				ExpenseToken: poolAmt.ExpenseToken,
				IncomeAmt:    poolAmt.IncomeAmt,
				ExpenseAmt:   poolAmt.ExpenseAmt,
			}
			buckets[key] = append(buckets[key], entry)
		}
	}

	if crossBlock {
		// Sort each bucket first by slotId, then by position
		for k := range buckets {
			sort.Slice(buckets[k], func(i, j int) bool {
				if buckets[k][i].Slot != buckets[k][j].Slot {
					return buckets[k][i].Slot < buckets[k][j].Slot
				}
				return buckets[k][i].Position < buckets[k][j].Position
			})
		}
	} else {
		// Sort each bucket by position only
		for k := range buckets {
			sort.Slice(buckets[k], func(i, j int) bool {
				return buckets[k][i].Position < buckets[k][j].Position
			})
		}
	}

	return buckets
}

func isEntriesConsecutive(es []PoolEntry, crossBlock bool) bool {
	if len(es) <= 1 {
		return true
	}

	if crossBlock {
		for i := 1; i < len(es); i++ {
			if es[i].Slot != es[i-1].Slot {
				return false
			}
			if es[i].Position != es[i-1].Position+1 {
				return false
			}
		}
	} else {
		for i := 1; i < len(es); i++ {
			if es[i].Position != es[i-1].Position+1 {
				return false
			}
		}
	}
	return true
}
