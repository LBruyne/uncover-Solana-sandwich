package sol

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"watcher/config"
	"watcher/types"
	"watcher/utils"
)

type InBlockSandwichFinder struct {
	Txs             types.Transactions
	Sandwiches      []*types.InBlockSandwich
	AmountThreshold uint

	// Internal states
	buckets                map[PoolKey][]PoolEntry
	confirmedSandwichTxIdx map[int]bool // TxIdx that have been confirmed as frontTx or backTx in any sandwich

	// Record the last evaluated sandwich info
	// Sandwich: A->B (front), A->B (victim), B->A (back)
	lastTokenA           string
	lastTokenB           string
	lastFrontTxEntries   []PoolEntry
	lastBackTxEntries    []PoolEntry
	lastVictimEntries    []PoolEntry
	perfect              bool
	relativeAmtDiffB     float64
	profitA              float64
	FrontFromTotalAmount float64
	FrontToTotalAmount   float64
	BackFromTotalAmount  float64
	BackToTotalAmount    float64
}

// Suppose a sandwich consists of front-run(s) A->B, victim(s) A->B, and back-run(s) B->A.
// Find detects in-block sandwiches in the provided transactions
func (f *InBlockSandwichFinder) Find() {
	f.Sandwiches = make([]*types.InBlockSandwich, 0)
	f.confirmedSandwichTxIdx = make(map[int]bool)
	// Build buckets for txs, keyed by (pool, fromToken, toToken)
	f.buckets = buildTxBuckets(f.Txs, false)

	// For each bucket, scan its reverse bucket to find possible sandwiches
	// For a pool in frontTx(s), A is incomeToken, B is expenseTokens; in backTx(s), B is incomeToken, A is expenseToken
	// Front direction: (pool, A, B)
	for key, frontTxBucket := range f.buckets {
		if len(frontTxBucket) == 0 {
			continue
		}
		// All transactions in txEntries interact with the same pool and the same token pair (pool, fromToken, toToken)
		// Back direction: (pool, B, A)
		// Scan all rev buckets to back-run tx
		revKey := PoolKey{PoolAddress: key.PoolAddress, IncomeToken: key.ExpenseToken, ExpenseToken: key.IncomeToken}
		backTxBucket, ok := f.buckets[revKey]
		if !ok || len(backTxBucket) == 0 {
			continue // No reverse bucket, cannot form sandwich
		}

		// For each frontTx in frontTxBucket,
		// 1. find other frontTx in frontTxBucket that may form multi-front-run sandwich (if any)
		// 2. find backTx(s) in backTxBucket that may form sandwich with frontTx
		// 3. Check the amount condition
		// 4. Find victim txs between front and back
		// 5. Record the sandwich if all conditions are met
		for i := 0; i < len(frontTxBucket); i++ {
			frontTxEntry := frontTxBucket[i]
			// An attacker may use multiple frontTxs.
			// Try to find other frontTx(s) accompanying this frontTx, if any
			frontTxEntries := f.collectFrontTxs(frontTxEntry, frontTxBucket)
			if len(frontTxEntries) == 0 {
				continue // No valid front-run tx found
			}

			// Try to find backTx(s) in that may form sandwich with current frontTx(s)
			backTxEntries := f.collectBackTxs(frontTxEntries, backTxBucket)
			if len(backTxEntries) == 0 {
				continue // No back-run candidate found
			}

			// All conditions met, record the sandwich
			// Every data structure is ready in f.last* fields
			f.RecordSandwich()
			// Reset last* fields
			f.ResetSandwichState()
		}
	}
}

func (f *InBlockSandwichFinder) ResetSandwichState() {
	f.lastFrontTxEntries = nil
	f.lastBackTxEntries = nil
	f.lastVictimEntries = nil
}

// / collectFrontTxs collects front-run transaction candidates from frontTxBucket that can accompany with the given frontTxEntry. Note that we consider multiple front-run transactions for sandwich attack.
func (f *InBlockSandwichFinder) collectFrontTxs(frontTxEntry PoolEntry, frontTxBucket []PoolEntry) []PoolEntry {
	res := make([]PoolEntry, 0)
	maxGap := config.SANDWICH_FRONTRUN_MAX_GAP // How long can two fornt-run txs be apart

	if f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[frontTxEntry.TxIdx] {
		return res // This tx has been confirmed as part of a sandwich
	}
	if tx := f.Txs[frontTxEntry.TxIdx]; tx == nil || tx.IsFailed || tx.IsVote {
		return res // Skip failed or vote transactions
	}

	signer := frontTxEntry.Signer
	res = append(res, frontTxEntry)
	lastPos := frontTxEntry.Position

	// frontTxBucket is sorted by (slot, position), so we can just scan forward
	for _, entry := range frontTxBucket {
		if entry.Position <= lastPos {
			continue
		}
		// Cannot be too far away, if too far, stop here
		if entry.Position-lastPos > maxGap {
			break
		}
		// Skip already confirmed tx in other sandwich
		// Skip failed or vote transactions
		if (f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[entry.TxIdx]) || f.Txs[entry.TxIdx] == nil || f.Txs[entry.TxIdx].IsFailed || f.Txs[entry.TxIdx].IsVote {
			continue
		}
		// Must have the same signer
		if entry.Signer != signer {
			continue
		}
		// Valid front-run tx accompanying with the given frontTxEntry
		res = append(res, entry)
		lastPos = entry.Position
	}
	return res
}

// collectBackTxs collects back-run transaction candidates from backTxBucket that can pair with the given frontTxEntry. Note that we consider multiple back-run transactions to match the front-run transaction(s).
func (f *InBlockSandwichFinder) collectBackTxs(frontTxEntries []PoolEntry, backTxBucket []PoolEntry) []PoolEntry {
	maxGap := config.SANDWICH_BACKRUN_MAX_GAP // How long can two back-run txs be apart

	// Start from the last front-run tx position + 2, since at least one victim tx is required
	lastFrontPos := frontTxEntries[len(frontTxEntries)-1].Position + 1

	// Scan forward in backTxBucket to find the first valid back-run tx
	for i, startBackEntry := range backTxBucket {
		if startBackEntry.Position <= lastFrontPos {
			continue
		}
		// Skip already confirmed tx in other sandwich
		// Skip failed or vote transactions
		if (f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[startBackEntry.TxIdx]) || f.Txs[startBackEntry.TxIdx] == nil || f.Txs[startBackEntry.TxIdx].IsFailed || f.Txs[startBackEntry.TxIdx].IsVote {
			continue
		}

		signer := startBackEntry.Signer
		candidateBackTxEntries := []PoolEntry{startBackEntry}
		lastPos := startBackEntry.Position

		// Try to find other back-run txs accompanying with this back-run tx
		for j := i + 1; j < len(backTxBucket); j++ {
			backEntry := backTxBucket[j]
			if backEntry.Position <= lastPos {
				continue
			}
			// Cannot be too far away, if too far, stop here
			if backEntry.Position-lastPos > maxGap {
				break
			}
			// Skip already confirmed tx in other sandwich
			// Skip failed or vote transactions
			if (f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[backEntry.TxIdx]) ||
				f.Txs[backEntry.TxIdx] == nil || f.Txs[backEntry.TxIdx].IsFailed || f.Txs[backEntry.TxIdx].IsVote {
				continue
			}
			// Must have the same signer, if meet different signer
			if backEntry.Signer != signer {
				continue
			}
			// Valid back-run tx accompanying with the given backEntry candidate
			candidateBackTxEntries = append(candidateBackTxEntries, backEntry)
			lastPos = backEntry.Position
		}
		// Valid back-run tx accompanying with the given frontTxEntry
		if f.Evaluate(frontTxEntries, candidateBackTxEntries) {
			// Mark all these txs as confirmed sandwich txs
			return candidateBackTxEntries
		}
	}
	return make([]PoolEntry, 0)
}

// Evaluate checks if the current front and back transactions form a sandwich based on amount condition
func (f *InBlockSandwichFinder) Evaluate(frontTxEntries []PoolEntry, backTxEntries []PoolEntry) bool {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
		return false
	}
	// Threshold is a percentage, e.g., 5 means 5%, allowed difference in total amount
	// TODO: how to identify similar-amount non-sandwich?
	threshold := f.AmountThreshold
	if frontTxEntries[0].IncomeToken != utils.SOL {
		threshold = threshold / 2 // Tighten the threshold for non-SOL token pairs
	}
	frontSigner := frontTxEntries[0].Signer
	backSigner := backTxEntries[0].Signer
	if frontSigner != backSigner {
		threshold = 2
	}

	// For a pool in frontTx(s), A is incomeToken, B is expenseTokens; in backTx(s), B is incomeToken, A is expenseToken
	// Check the amount condition: B in front and back have valid and similar trading amounts (within threshold)
	var frontAmtB float64
	for _, fe := range frontTxEntries {
		if fe.ExpenseAmt < 0 {
			frontAmtB += -fe.ExpenseAmt
		}
	}
	var backAmtB float64
	for _, be := range backTxEntries {
		if be.IncomeAmt > 0 {
			backAmtB += be.IncomeAmt
		}
	}
	// Check backAmtB <= frontAmtB
	if backAmtB > frontAmtB {
		return false // Back amount B cannot be greater than front amount B
	}
	// Check amount similarity
	similar, relativeAmtDiff := f.HasSimilarAmount(frontAmtB, backAmtB, threshold)
	if !similar {
		return false // Amount not similar enough
	}
	perfect := (relativeAmtDiff <= 0.001) // Perfect if relative difference ~= 0

	// Check victim txs between front and back
	victimEntries := f.collectVictimEntries(frontTxEntries, backTxEntries)
	if len(victimEntries) == 0 {
		return false // No victim txs found
	}

	// All conditions met, record the sandwich
	f.lastTokenA = frontTxEntries[0].IncomeToken
	f.lastTokenB = frontTxEntries[0].ExpenseToken
	f.lastFrontTxEntries = frontTxEntries
	f.lastBackTxEntries = backTxEntries
	f.lastVictimEntries = victimEntries
	f.perfect = perfect
	f.relativeAmtDiffB = relativeAmtDiff
	f.FrontToTotalAmount = frontAmtB
	f.BackFromTotalAmount = backAmtB

	var frontAmtA float64
	for _, fe := range frontTxEntries {
		if fe.IncomeAmt > 0 {
			frontAmtA += fe.IncomeAmt
		}
	}
	var backAmtA float64
	for _, be := range backTxEntries {
		if be.ExpenseAmt < 0 {
			backAmtA += -be.ExpenseAmt
		}
	}
	f.profitA = backAmtA - frontAmtA
	f.FrontFromTotalAmount = frontAmtA
	f.BackToTotalAmount = backAmtA
	return true
}

func (f *InBlockSandwichFinder) collectVictimEntries(frontTxEntries, backTxEntries []PoolEntry) []PoolEntry {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
		return make([]PoolEntry, 0)
	}
	// Victim txs must be between front and back
	frontEndPos := frontTxEntries[len(frontTxEntries)-1].Position
	backBeginPos := backTxEntries[0].Position
	if backBeginPos <= frontEndPos+1 {
		return make([]PoolEntry, 0) // No space for victim txs
	}
	// Victim must have the same pool and trading direction as front
	frontKey := PoolKey{
		PoolAddress:  frontTxEntries[0].PoolAddress,
		IncomeToken:  frontTxEntries[0].IncomeToken,
		ExpenseToken: frontTxEntries[0].ExpenseToken,
	}
	frontTxBucket := f.buckets[frontKey]
	// Victim must have different signer from front and back
	frontSigner := frontTxEntries[0].Signer
	backSigner := backTxEntries[0].Signer

	victims := make([]PoolEntry, 0)
	for _, e := range frontTxBucket {
		if f.Txs[e.TxIdx] == nil || f.Txs[e.TxIdx].IsFailed || f.Txs[e.TxIdx].IsVote {
			// Note that we do not skip already confirmed sadnwich tx here, because we assume a victim tx may be used in multiple sandwiches
			continue // Skip failed or vote transactions
		}
		// Must be between front and back
		if e.Position <= frontEndPos || e.Position >= backBeginPos {
			continue
		}
		// Must have different signer from front and back
		if e.Signer == frontSigner || e.Signer == backSigner {
			continue
		}
		// Maybe (useless) deadcode
		if !(e.IncomeAmt > 0 && e.ExpenseAmt < 0) {
			continue
		}
		victims = append(victims, e)
	}
	return victims
}

// HasSimilarAmount checks if two amounts are similar within the configured threshold
// Amount check: |\sum{|frontTx.B_Out|} - \sum{|backTx.B_In|}| / max(\sum{|frontTx.B_Out|}, \sum{|backTx.B_In|}) <= threshold
func (f *InBlockSandwichFinder) HasSimilarAmount(frontAmt, backAmt float64, threshold uint) (bool, float64) {
	if frontAmt <= 0 || backAmt <= 0 {
		return false, -1.0
	}

	relativeDiff := f.GetAmountRelativeDifference(frontAmt, backAmt)
	if relativeDiff < 0 {
		return false, -1.0
	}

	return relativeDiff <= float64(threshold)/100.0, relativeDiff
}

func (f *InBlockSandwichFinder) GetAmountRelativeDifference(frontAmt, backAmt float64) float64 {
	if frontAmt <= 0 || backAmt <= 0 {
		return -1.0
	}
	return math.Abs(frontAmt-backAmt) / math.Max(frontAmt, backAmt)
}

func (f *InBlockSandwichFinder) RecordSandwich() {
	if len(f.lastFrontTxEntries) == 0 || len(f.lastBackTxEntries) == 0 || len(f.lastVictimEntries) == 0 {
		return
	}

	sandwichId := makeSandwichID(f.Txs[f.lastFrontTxEntries[0].TxIdx].Signature, f.Txs[f.lastBackTxEntries[0].TxIdx].Signature)

	// Make SandwichTxs
	frontTxs := make([]*types.SandwichTx, 0, len(f.lastFrontTxEntries))
	for _, fe := range f.lastFrontTxEntries {
		frontTxs = append(frontTxs, f.makeSandwichTx(sandwichId, fe, "frontRun"))
	}
	backTxs := make([]*types.SandwichTx, 0, len(f.lastBackTxEntries))
	for _, be := range f.lastBackTxEntries {
		backTxs = append(backTxs, f.makeSandwichTx(sandwichId, be, "backRun"))
	}
	victimTxs := make([]*types.SandwichTx, 0, len(f.lastVictimEntries))
	for _, ve := range f.lastVictimEntries {
		victimTxs = append(victimTxs, f.makeSandwichTx(sandwichId, ve, "victim"))
	}
	// Append total and diff amount for last tx in frontTxs and backTxs
	if len(frontTxs) > 0 {
		lf := frontTxs[len(frontTxs)-1]
		lf.SandwichTxTokenInfo.FromTotalAmount = f.FrontFromTotalAmount
		lf.SandwichTxTokenInfo.ToTotalAmount = f.FrontToTotalAmount
	}
	if len(backTxs) > 0 {
		lb := backTxs[len(backTxs)-1]
		lb.SandwichTxTokenInfo.FromTotalAmount = f.BackFromTotalAmount
		lb.SandwichTxTokenInfo.ToTotalAmount = f.BackToTotalAmount
		// Diff amount
		lb.SandwichTxTokenInfo.DiffA = f.BackToTotalAmount - f.FrontFromTotalAmount
		lb.SandwichTxTokenInfo.DiffB = f.FrontToTotalAmount - f.BackFromTotalAmount
	}

	// Signer
	frontSigner := f.lastFrontTxEntries[0].Signer
	backSigner := f.lastBackTxEntries[0].Signer
	signerSame := (frontSigner == backSigner)

	// Slot and timestamp
	slot := f.Txs[f.lastFrontTxEntries[0].TxIdx].Slot
	timestamp := f.Txs[f.lastFrontTxEntries[0].TxIdx].Timestamp

	// Make InBlockSandwich
	s := &types.InBlockSandwich{
		Sandwich: types.Sandwich{
			SandwichID: sandwichId,
			TokenA:     f.lastTokenA,
			TokenB:     f.lastTokenB,
			// Block info
			CrossBlock: false,
			// Consecutiveness
			Consecutive:       isSandwichConsecutive(f.lastFrontTxEntries, f.lastVictimEntries, f.lastBackTxEntries),
			FrontConsecutive:  isEntriesConsecutive(f.lastFrontTxEntries, false),
			BackConsecutive:   isEntriesConsecutive(f.lastBackTxEntries, false),
			VictimConsecutive: isEntriesConsecutive(f.lastVictimEntries, false),
			// Signer/owner/ata info
			SignerSame: signerSame,
			OwnerSame:  false, // TODO:
			ATASame:    false, // TODO:
			// Amount info
			Perfect:       f.perfect,
			RelativeDiffB: f.relativeAmtDiffB,
			ProfitA:       f.profitA,

			// Counts
			MultiFrontRun: len(f.lastFrontTxEntries) > 1,
			MultiBackRun:  len(f.lastBackTxEntries) > 1,
			MultiVictim:   len(f.lastVictimEntries) > 1,
			FrontCount:    uint16(len(frontTxs)),
			BackCount:     uint16(len(backTxs)),
			VictimCount:   uint16(len(victimTxs)),
			// SandwichTxs
			FrontRun: frontTxs,
			BackRun:  backTxs,
			Victims:  victimTxs,
		},
		Slot:      slot,
		Timestamp: timestamp,
	}

	f.Sandwiches = append(f.Sandwiches, s)
	// Mark all these txs as confirmed sandwich txs
	if f.confirmedSandwichTxIdx == nil {
		f.confirmedSandwichTxIdx = make(map[int]bool)
	}
	for _, e := range f.lastFrontTxEntries {
		f.confirmedSandwichTxIdx[e.TxIdx] = true
	}
	for _, e := range f.lastBackTxEntries {
		f.confirmedSandwichTxIdx[e.TxIdx] = true
	}
}

// makeSandwichTx makes a SandwichTx from a PoolEntry and kind ("frontRun", "victim", "backRun")
func (f *InBlockSandwichFinder) makeSandwichTx(sandwichId string, entry PoolEntry, kind string) *types.SandwichTx {
	orig := f.Txs[entry.TxIdx]
	var stx *types.SandwichTx
	switch kind {
	case "frontRun", "victim", "backRun":
		stx = &types.SandwichTx{
			SandwichID:  sandwichId,
			Transaction: *orig,
			SandwichTxTokenInfo: types.SandwichTxTokenInfo{
				FromToken:  entry.IncomeToken,
				ToToken:    entry.ExpenseToken,
				FromAmount: math.Abs(entry.IncomeAmt),
				ToAmount:   math.Abs(entry.ExpenseAmt),
			},
			InBundle: false, // default false
			Type:     kind,  // "frontRun" / "backRun" / "victim"
		}
	default:
		return nil
	}

	return stx
}

// makeSandwichID makes a unique ID for a sandwich based on the front and back transaction signatures
func makeSandwichID(frontSig, backSig string) string {
	h := sha256.Sum256([]byte(frontSig + ":" + backSig))
	return hex.EncodeToString(h[:])
}

// isSandwichConsecutive checks if the given front, victim, and back entries are consecutive in terms of position
func isSandwichConsecutive(frontTxs, victimTxs, backTxs []PoolEntry) bool {
	if len(frontTxs) == 0 || len(victimTxs) == 0 || len(backTxs) == 0 {
		return false
	}

	if frontTxs[len(frontTxs)-1].Position+1 != victimTxs[0].Position {
		return false
	}
	if victimTxs[len(victimTxs)-1].Position+1 != backTxs[0].Position {
		return false
	}
	return true
}
