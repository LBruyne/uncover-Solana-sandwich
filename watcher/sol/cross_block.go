package sol

import (
	"math"
	"watcher/config"
	"watcher/logger"
	"watcher/types"
	"watcher/utils"

	MapSet "github.com/deckarep/golang-set/v2"
)

// CrossBlockSandwichFinder finds cross-block sandwiches using slot buckets
type CrossBlockSandwichFinder struct {
	Txs             types.Transactions
	Blocks          types.Blocks
	Sandwiches      []*types.CrossBlockSandwich
	AmountThreshold uint

	// Internal states
	blockMap               map[uint64]*types.Block
	buckets                map[PoolKey][]PoolEntry
	confirmedSandwichTxIdx map[int]bool // Use signature as key for cross-block

	// Record the last evaluated sandwich info
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

func NewCrossBlockSandwichFinder(blocks types.Blocks, amountThreshold uint) *CrossBlockSandwichFinder {
	f := &CrossBlockSandwichFinder{
		Blocks:                 blocks,
		AmountThreshold:        amountThreshold,
		blockMap:               make(map[uint64]*types.Block),
		confirmedSandwichTxIdx: make(map[int]bool),
		Sandwiches:             make([]*types.CrossBlockSandwich, 0),
		Txs:                    make(types.Transactions, 0),
	}

	for _, b := range blocks {
		f.blockMap[b.Slot] = b
	}

	for _, b := range blocks {
		for _, tx := range b.Txs {
			copied := *tx
			f.Txs = append(f.Txs, &copied)
		}
	}

	return f
}

// Find detects cross-block sandwiches using the slot buckets
func (f *CrossBlockSandwichFinder) Find() {
	f.Sandwiches = make([]*types.CrossBlockSandwich, 0)
	f.confirmedSandwichTxIdx = make(map[int]bool)
	f.buckets = buildTxBuckets(f.Txs, true)

	if len(f.Txs) > 0 {
		minSlot, maxSlot := f.Txs[0].Slot, f.Txs[0].Slot
		for _, tx := range f.Txs {
			if tx.Slot < minSlot {
				minSlot = tx.Slot
			}
			if tx.Slot > maxSlot {
				maxSlot = tx.Slot
			}
		}
		logger.SolLogger.Info(
			"[CrossBlockSandwichFinder] Searching sandwich across slots",
			"minSlot", minSlot,
			"maxSlot", maxSlot,
		)
	}

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

// / collectFrontTxs collects front-run transaction candidates from frontTxBucket that can accompany with the given frontTxEntry. Note that we consider multiple front-run transactions for sandwich attack.
func (f *CrossBlockSandwichFinder) collectFrontTxs(frontTxEntry PoolEntry, frontTxBucket []PoolEntry) []PoolEntry {
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
	lastGlobalPos := frontTxEntry.TxIdx // Cross-block sandwich, use global position

	// frontTxBucket is sorted by (slot, position), so we can just scan forward
	for _, entry := range frontTxBucket {
		if entry.TxIdx <= lastGlobalPos {
			continue
		}
		// Cannot be too far away, if too far, stop here
		if entry.TxIdx-lastGlobalPos > maxGap {
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
		lastGlobalPos = entry.TxIdx
	}
	return res
}

// collectBackTxs collects back-run transaction candidates from backTxBucket that can pair with the given frontTxEntry. Note that we consider multiple back-run transactions to match the front-run transaction(s).
func (f *CrossBlockSandwichFinder) collectBackTxs(frontTxEntries []PoolEntry, backTxBucket []PoolEntry) []PoolEntry {
	maxGap := config.SANDWICH_BACKRUN_MAX_GAP // How long can two back-run txs be apart

	// Start from the last front-run tx position + 2, since at least one victim tx is required
	lastFrontPos := frontTxEntries[len(frontTxEntries)-1].TxIdx + 1

	// Scan forward in backTxBucket to find the first valid back-run tx
	for i, startBackEntry := range backTxBucket {

		if startBackEntry.TxIdx <= lastFrontPos {
			continue
		}
		// Skip already confirmed tx in other sandwich
		// Skip failed or vote transactions
		if (f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[startBackEntry.TxIdx]) || f.Txs[startBackEntry.TxIdx] == nil || f.Txs[startBackEntry.TxIdx].IsFailed || f.Txs[startBackEntry.TxIdx].IsVote {
			continue
		}

		signer := startBackEntry.Signer
		candidateBackTxEntries := []PoolEntry{startBackEntry}
		lastGlobalPos := startBackEntry.TxIdx

		// Try to find other back-run txs accompanying with this back-run tx
		for j := i + 1; j < len(backTxBucket); j++ {
			backEntry := backTxBucket[j]
			if backEntry.TxIdx <= lastGlobalPos {
				continue
			}
			// Cannot be too far away, if too far, stop here
			if backEntry.TxIdx-lastGlobalPos > maxGap {
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
			lastGlobalPos = backEntry.TxIdx
		}

		// if f.Txs[frontTxEntries[0].TxIdx].Signature == "6ZTDa1tbT22vsAcXoURNy58BzSiRv8oo6ewGxdA2M8WXUyr4swkGR54LfTiUz77EMwv7sLUT7KAgj5UM7ro8tWn" {
		// 	fmt.Printf("Debug: found specific backTx: %+v\n", candidateBackTxEntries)
		// }

		// Possible valid back-run tx accompanying with the given frontTxEntries
		// Check if they form a sandwich: amount, victim txs, etc.
		if f.Evaluate(frontTxEntries, candidateBackTxEntries) {
			// Mark all these txs as confirmed sandwich txs
			return candidateBackTxEntries
		}
	}
	return make([]PoolEntry, 0)
}

// Evaluate checks if the current front and back transactions form a sandwich based on amount condition
func (f *CrossBlockSandwichFinder) Evaluate(frontTxEntries []PoolEntry, backTxEntries []PoolEntry) bool {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
		return false
	}

	// Cross-block sandwich needs front and back txs in different slots
	if frontTxEntries[0].Slot == backTxEntries[len(backTxEntries)-1].Slot {
		return false
	}

	// Sandwich from A to B and back to A
	tokenA := frontTxEntries[0].IncomeToken
	if tokenA == "" || tokenA != backTxEntries[0].ExpenseToken {
		return false
	}
	tokenB := frontTxEntries[0].ExpenseToken
	if tokenB == "" || tokenB != backTxEntries[0].IncomeToken {
		return false
	}
	// Signer
	frontSigner := frontTxEntries[0].Signer
	backSigner := backTxEntries[0].Signer

	// Check pool condition
	maxPoolsCnt := 1
	if frontSigner != backSigner {
		maxPoolsCnt += 1
	}
	for _, fe := range frontTxEntries {
		ftx := f.Txs[fe.TxIdx]
		if ftx.RelatedPools.Cardinality() > maxPoolsCnt {
			return false // Front tx interacts with too many pools
		}
	}
	for _, be := range backTxEntries {
		btx := f.Txs[be.TxIdx]
		if btx.RelatedPools.Cardinality() > maxPoolsCnt {
			return false // Back tx interacts with too many pools
		}
	}

	// Threshold is a percentage, e.g., 5 means 5%, allowed difference in total amount
	threshold := f.AmountThreshold
	// For a pool in frontTx(s), A is incomeToken, B is expenseTokens; in backTx(s), B is incomeToken, A is expenseToken
	// Check the amount condition: B in front and back have valid and similar trading amounts (within threshold)
	var frontAmtB float64
	for _, fe := range frontTxEntries {
		// tokenB is frontTx.ExpenseToken
		if fe.ExpenseAmt < 0 {
			frontAmtB += -fe.ExpenseAmt
		}
	}
	var backAmtB float64
	for _, be := range backTxEntries {
		// tokenB is backTx.IncomeToken
		if be.IncomeAmt > 0 {
			backAmtB += be.IncomeAmt
		}
	}

	// Check backAmtB <= frontAmtB
	if backAmtB-frontAmtB > utils.EPSILON {
		return false // Back amount B cannot be greater than front amount B
	}
	// Check amount similarity
	similar, relativeAmtDiff := f.HasSimilarAmount(frontAmtB, backAmtB, float64(threshold))
	if !similar {
		return false // Amount not similar enough
	}
	// Perfect if relative difference = 0 if tokenB is SPL token,
	// or relative difference ~= 0 if tokenB is SOL (to tolerate some rent/exchange fee)
	var perfect bool
	if tokenB == utils.SOL {
		perfect = relativeAmtDiff <= 0.01 // 1% tolerance for SOL
	} else {
		perfect = relativeAmtDiff == 0.0
	}

	// Check victim txs between front and back
	victimEntries := f.collectVictimEntries(frontTxEntries, backTxEntries)
	if len(victimEntries) == 0 {
		return false // No victim txs found
	}

	// If signers of frontTxs and backTxs are the same, it is confirmed right now
	if frontSigner != backSigner {
		// Different signers, cannot be confirmed right now
		// We check if
		// in lastFrontTx, the post-balance of tokenB of the attackers, is similar to
		// in firstBackTx, the pre-balance of tokenB of the attackers
		postBalanceBInFrtTxs := map[string]float64{}
		for _, frtTxEntry := range frontTxEntries {
			frtTx := f.Txs[frtTxEntry.TxIdx]
			for owner, bc := range frtTx.OwnerBalanceChanges {
				if bc[tokenB].TotalAmount > 0 {
					// This owner may be the attacker
					postBalanceBInFrtTxs[owner] = frtTx.OwnerPostBalances[owner][tokenB]
				}
			}
		}
		preBalanceBInBckTxs := map[string]float64{}
		for _, bckTxEntry := range backTxEntries {
			bckTx := f.Txs[bckTxEntry.TxIdx]
			for owner, bc := range bckTx.OwnerBalanceChanges {
				if bc[tokenB].TotalAmount < 0 {
					// This owner may be the attacker
					preBalanceBInBckTxs[owner] = bckTx.OwnerPreBalances[owner][tokenB]
				}
			}
		}
		var atkPostBalanceAfterFrtRun float64
		for _, pB := range postBalanceBInFrtTxs {
			atkPostBalanceAfterFrtRun += pB
		}
		var atkPreBalanceBeforeBckRun float64
		for _, pB := range preBalanceBInBckTxs {
			atkPreBalanceBeforeBckRun += pB
		}
		// Check if the balance is similar
		if !(math.Abs(atkPostBalanceAfterFrtRun-atkPreBalanceBeforeBckRun) < utils.EPSILON) {
			return false // Attacker's balance of tokenB not similar enough before backTx(s) and after frontTx(s)
		}
	}

	// All conditions met, record the sandwich
	firstSlot := frontTxEntries[0].Slot
	lastSlot := backTxEntries[len(backTxEntries)-1].Slot
	if firstSlot != lastSlot {
		f.lastTokenA = tokenA
		f.lastTokenB = tokenB
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
	}

	return true
}

func (f *CrossBlockSandwichFinder) collectVictimEntries(frontTxEntries, backTxEntries []PoolEntry) []PoolEntry {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
		return make([]PoolEntry, 0)
	}
	// Victim txs must be between front and back
	frontEndPos := frontTxEntries[len(frontTxEntries)-1].TxIdx
	backBeginPos := backTxEntries[0].TxIdx
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
			// Note that we do not skip already confirmed sandwich tx here, because we assume a victim tx may be used in multiple sandwiches
			continue // Skip failed or vote transactions
		}
		// Must be between front and back
		if e.TxIdx <= frontEndPos || e.TxIdx >= backBeginPos {
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
func (f *CrossBlockSandwichFinder) HasSimilarAmount(frontAmt, backAmt float64, threshold float64) (bool, float64) {
	if frontAmt <= 0 || backAmt <= 0 {
		return false, -1.0
	}

	relativeDiff := f.GetAmountRelativeDifference(frontAmt, backAmt)
	if relativeDiff < 0 {
		return false, -1.0
	}

	return relativeDiff <= threshold/100.0, relativeDiff
}

func (f *CrossBlockSandwichFinder) GetAmountRelativeDifference(frontAmt, backAmt float64) float64 {
	if frontAmt <= 0 || backAmt <= 0 {
		return -1.0
	}
	return math.Abs(frontAmt-backAmt) / math.Max(frontAmt, backAmt)
}

func (f *CrossBlockSandwichFinder) RecordSandwich() {
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
	lastFrontTx := f.Txs[f.lastFrontTxEntries[len(f.lastFrontTxEntries)-1].TxIdx]
	firstBackTx := f.Txs[f.lastBackTxEntries[0].TxIdx]
	frontSigner := lastFrontTx.Signer
	backSigner := firstBackTx.Signer
	signerSame := (frontSigner == backSigner)
	// Owner
	frontOwners := MapSet.NewSet[string]()
	backOwners := MapSet.NewSet[string]()
	for _, frtSTx := range frontTxs {
		frontOwners.Append(frtSTx.OwnersOfB...)
	}
	for _, bckSTx := range backTxs {
		backOwners.Append(bckSTx.OwnersOfB...)
	}
	ownerSame := frontOwners.IsSuperset(backOwners) // If all back owners are in front owners, consider owner same (front owners may contain fee recipients)

	// Slot and timestamp
	slot := f.Txs[f.lastFrontTxEntries[0].TxIdx].Slot
	timestamp := f.Txs[f.lastFrontTxEntries[0].TxIdx].Timestamp

	// Make CrossBlockSandwich
	s := &types.CrossBlockSandwich{
		Sandwich: types.Sandwich{
			SandwichID: sandwichId,
			TokenA:     f.lastTokenA,
			TokenB:     f.lastTokenB,
			// Block info
			CrossBlock: true,
			// Consecutiveness
			Consecutive:       isSandwichConsecutive(f.lastFrontTxEntries, f.lastVictimEntries, f.lastBackTxEntries),
			FrontConsecutive:  isEntriesConsecutive(f.lastFrontTxEntries, false),
			BackConsecutive:   isEntriesConsecutive(f.lastBackTxEntries, false),
			VictimConsecutive: isEntriesConsecutive(f.lastVictimEntries, false),
			// Signer/owner/ata info
			SignerSame: signerSame,
			OwnerSame:  ownerSame,
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
func (f *CrossBlockSandwichFinder) makeSandwichTx(sandwichId string, entry PoolEntry, kind string) *types.SandwichTx {
	orig := f.Txs[entry.TxIdx]
	stx := &types.SandwichTx{
		SandwichID:  sandwichId,
		Transaction: *orig,
		SandwichTxTokenInfo: types.SandwichTxTokenInfo{
			FromToken:            entry.IncomeToken,
			ToToken:              entry.ExpenseToken,
			FromAmount:           math.Abs(entry.IncomeAmt),
			ToAmount:             math.Abs(entry.ExpenseAmt),
			OwnersOfB:            []string{},
			AttackerPreBalanceB:  0.0,
			AttackerPostBalanceB: 0.0,
		},
		InBundle: false, // default false
		Type:     kind,  // "frontRun" / "backRun" / "victim"
	}
	switch kind {
	case "frontRun":
		for owner, bc := range orig.OwnerBalanceChanges {
			if bc[f.lastTokenB].TotalAmount > 0 {
				// This owner may be the attacker
				stx.SandwichTxTokenInfo.OwnersOfB = append(stx.SandwichTxTokenInfo.OwnersOfB, owner)
				stx.SandwichTxTokenInfo.AttackerPreBalanceB += orig.OwnerPreBalances[owner][f.lastTokenB]
				stx.SandwichTxTokenInfo.AttackerPostBalanceB += orig.OwnerPostBalances[owner][f.lastTokenB]
			}
		}
	case "backRun":
		for owner, bc := range orig.OwnerBalanceChanges {
			if bc[f.lastTokenB].TotalAmount < 0 {
				// This owner may be the attacker
				stx.SandwichTxTokenInfo.OwnersOfB = append(stx.SandwichTxTokenInfo.OwnersOfB, owner)
				stx.SandwichTxTokenInfo.AttackerPreBalanceB += orig.OwnerPreBalances[owner][f.lastTokenB]
				stx.SandwichTxTokenInfo.AttackerPostBalanceB += orig.OwnerPostBalances[owner][f.lastTokenB]
			}
		}
	case "victim":
		// Nothing special for victim
	default:
		// Unknown kind, do nothing
	}

	return stx
}

func (f *CrossBlockSandwichFinder) ResetSandwichState() {
	f.lastFrontTxEntries = nil
	f.lastBackTxEntries = nil
	f.lastVictimEntries = nil
}
