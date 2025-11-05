package sol

import (
	"math"
	"watcher/config"
	"watcher/types"
	"watcher/utils"

	MapSet "github.com/deckarep/golang-set/v2"
)

type OldInBlockSandwichFinder struct {
	Txs             types.Transactions
	Sandwiches      []*types.InBlockSandwich
	AmountThreshold uint

	// Internal states
	confirmedSandwichTxIdx map[int]bool // TxIdx that have been confirmed as frontTx or backTx in any sandwich

	// Record the last evaluated sandwich info
	// Sandwich: A->B (front), A->B (victim), B->A (back)
	lastTokenA           string
	lastTokenB           string
	lastFrontTxEntries   []PoolEntry
	lastBackTxEntries    []PoolEntry
	lastVictimEntries    []PoolEntry
	lastTransferTxs      []*types.Transaction
	perfect              bool
	relativeAmtDiffB     float64
	profitA              float64
	FrontFromTotalAmount float64
	FrontToTotalAmount   float64
	BackFromTotalAmount  float64
	BackToTotalAmount    float64
}

func getKeyEntry(tx *types.Transaction, idx int) (PoolKey, PoolEntry) {
	if tx.RelatedPools.Cardinality() == 0 || tx.RelatedTokens.Cardinality() < 2 {
		return PoolKey{}, PoolEntry{}
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
		return PoolKey{PoolAddress: pool, IncomeToken: poolAmt.IncomeToken, ExpenseToken: poolAmt.ExpenseToken}, PoolEntry{
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
	}

	return PoolKey{}, PoolEntry{}
}

// Suppose a sandwich consists of front-run(s) A->B, victim(s) A->B, and back-run(s) B->A.
// Find detects in-block sandwiches in the provided transactions
func (f *OldInBlockSandwichFinder) Find() {
	f.Sandwiches = make([]*types.InBlockSandwich, 0)
	f.confirmedSandwichTxIdx = make(map[int]bool)

	// For a pool in frontTx(s), A is incomeToken, B is expenseTokens; in backTx(s), B is incomeToken, A is expenseToken
	// Front direction: (pool, A, B)
	for idx, ftx := range f.Txs {
		if ftx == nil || ftx.IsFailed || ftx.IsVote {
			continue
		}
		if f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[idx] {
			continue
		}

		frontKey, frontEntry := getKeyEntry(ftx, idx)
		if frontKey.PoolAddress == "" || frontKey.IncomeToken == "" || frontKey.ExpenseToken == "" {
			continue
		}

		frontTxEntries := []PoolEntry{frontEntry}

		backTxEntries := make([]PoolEntry, 0)
		for i := idx + 1; i < len(f.Txs); i++ {
			btx := f.Txs[i]
			if btx == nil || btx.IsFailed || btx.IsVote {
				continue
			}
			if f.confirmedSandwichTxIdx != nil && f.confirmedSandwichTxIdx[i] {
				continue
			}
			backKey, backEntry := getKeyEntry(btx, i)
			if backKey.PoolAddress == "" || backKey.IncomeToken == "" || backKey.ExpenseToken == "" {
				continue
			}
			// Back direction: (pool, B, A)
			if backKey.PoolAddress != frontKey.PoolAddress || backKey.IncomeToken != frontKey.ExpenseToken || backKey.ExpenseToken != frontKey.IncomeToken {
				continue
			}
			candidateBackTxEntries := []PoolEntry{backEntry}

			if f.Evaluate(frontTxEntries, candidateBackTxEntries) {
				backTxEntries = candidateBackTxEntries
				break
			}
		}

		if len(backTxEntries) == 0 {
			continue
		}

		// All conditions met, record the sandwich
		// Every data structure is ready in f.last* fields
		f.RecordSandwich()
		// Reset last* fields
		f.ResetSandwichState()
	}
}

func (f *OldInBlockSandwichFinder) ResetSandwichState() {
	f.lastFrontTxEntries = nil
	f.lastBackTxEntries = nil
	f.lastVictimEntries = nil
	f.lastTransferTxs = nil
}

func (f *OldInBlockSandwichFinder) Evaluate(frontTxEntries []PoolEntry, backTxEntries []PoolEntry) bool {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
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

	// if f.Txs[frontTxEntries[0].TxIdx].Signature == "6ZTDa1tbT22vsAcXoURNy58BzSiRv8oo6ewGxdA2M8WXUyr4swkGR54LfTiUz77EMwv7sLUT7KAgj5UM7ro8tWn" {
	// 	fmt.Printf("Debug: amounts of specific frontTx/backTx: frontAmtB=%.10f, backAmtB=%.10f\n", frontAmtB, backAmtB)
	// }

	// Check frontAmtB > backAmtB, using FloatRound to avoid precision issue
	if tokenB != utils.SOL && utils.FloatRound(frontAmtB, 3) < utils.FloatRound(backAmtB, 3) {
		return false // Invalid trading amount
	}
	// Check amount similarity: |\sum{|frontTx.B_Out|} - \sum{|backTx.B_In|}| / max(\sum{|frontTx.B_Out|}, \sum{|backTx.B_In|}) <= threshold
	similar, relativeAmtDiff := f.HasSimilarAmount(frontAmtB, backAmtB, float64(threshold))
	if !similar {
		return false // Amount not similar enough
	}
	// Perfect if relative difference = 0 if tokenB is SPL token,
	// or relative difference ~= 0 if tokenB is SOL (to tolerate some rent/exchange fee)
	var perfect bool
	if tokenB == utils.SOL {
		perfect = relativeAmtDiff <= (config.SANDWICH_AMOUNT_SOL_TOLERANCE / 100) // tolerance for SOL
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
		// Check owner condition
		ownersOfBInFrtTxs := MapSet.NewSet[string]()
		for _, frtTxEntry := range frontTxEntries {
			frtTx := f.Txs[frtTxEntry.TxIdx]
			for owner, bc := range frtTx.OwnerBalanceChanges {
				if bc[tokenB].TotalAmount > 0 {
					// This owner may be the attacker
					ownersOfBInFrtTxs.Add(owner)
				}
			}
		}
		ownersOfBInBckTxs := MapSet.NewSet[string]()
		for _, bckTxEntry := range backTxEntries {
			bckTx := f.Txs[bckTxEntry.TxIdx]
			for owner, bc := range bckTx.OwnerBalanceChanges {
				if bc[tokenB].TotalAmount < 0 {
					// This owner may be the attacker
					ownersOfBInBckTxs.Add(owner)
				}
			}
		}
		// Owner same, consider confirmed
		if !ownersOfBInFrtTxs.IsSuperset(ownersOfBInBckTxs) {
			// Owner different, need further check
			// We check if there is a transfer tx of tokenB from attackers to others, where tokenB is transferred from owners in frontTx(s) to owners in backTx(s)
			// Search txs between front and back
			transferTxs := make([]*types.Transaction, 0)
			for i := frontTxEntries[len(frontTxEntries)-1].Position + 1; i < backTxEntries[0].Position; i++ {
				tx := f.Txs[i]
				if tx == nil || tx.IsFailed || tx.IsVote {
					continue // Skip failed or vote transactions
				}

				// Tx has no pool
				if tx.RelatedPools.Cardinality() != 0 {
					continue
				}

				// Only one owner increases tokenB and only one owner decreases tokenB
				increaseBOwners := make([]string, 0)
				decreaseBOwners := make([]string, 0)
				for owner, bc := range tx.OwnerBalanceChanges {
					if bc[tokenB].TotalAmount > 0 {
						increaseBOwners = append(increaseBOwners, owner)
					} else if bc[tokenB].TotalAmount < 0 {
						decreaseBOwners = append(decreaseBOwners, owner)
					}
				}
				if len(increaseBOwners) != 1 || len(decreaseBOwners) != 1 {
					continue // More than one owner increases or decreases tokenB, cannot confirm
				}
				increaseBOwner := increaseBOwners[0]
				decreaseBOwner := decreaseBOwners[0]

				// Check if the most decrease of tokenB is from front owners and the most increase of tokenB is from back owners, which forms a transfer from front owners to back owners
				if ownersOfBInFrtTxs.Contains(decreaseBOwner) && ownersOfBInBckTxs.Contains(increaseBOwner) {
					// Found a transfer tx of tokenB from frt owners to bck owners
					transferTxs = append(transferTxs, tx)
				}
			}
			if len(transferTxs) == 0 {
				return false // No transfer tx found, cannot confirm this sandwich
			}

			f.lastTransferTxs = transferTxs
		}
	}

	// All conditions met, record the sandwich
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
	var estimateBackAmtA = frontAmtB * backAmtA / backAmtB // Estimate backAmtA based on frontAmtB and backAmtB, in case backAmtB != frontAmtB
	f.profitA = estimateBackAmtA - frontAmtA
	f.FrontFromTotalAmount = frontAmtA
	f.BackToTotalAmount = backAmtA
	return true
}

func (f *OldInBlockSandwichFinder) collectVictimEntries(frontTxEntries, backTxEntries []PoolEntry) []PoolEntry {
	if len(frontTxEntries) == 0 || len(backTxEntries) == 0 {
		return make([]PoolEntry, 0)
	}
	// Victim txs must be between front and back
	frontEndPos := frontTxEntries[len(frontTxEntries)-1].Position
	backBeginPos := backTxEntries[0].Position
	if backBeginPos <= frontEndPos+1 {
		return make([]PoolEntry, 0) // No space for victim txs
	}
	// Victim must have different signer from front and back
	frontSigner := frontTxEntries[0].Signer
	backSigner := backTxEntries[0].Signer

	victims := make([]PoolEntry, 0)
	for i := frontTxEntries[0].TxIdx + 1; i < backTxEntries[0].TxIdx; i++ {
		victimTx := f.Txs[i]
		if victimTx == nil || victimTx.IsFailed || victimTx.IsVote {
			// Note that we do not skip already confirmed sandwich tx here, because we assume a victim tx may be used in multiple sandwiches
			continue // Skip failed or vote transactions
		}
		// Must be between front and back
		if victimTx.Position <= frontEndPos || victimTx.Position >= backBeginPos {
			continue
		}
		// Must have different signer from front and back
		if victimTx.Signer == frontSigner || victimTx.Signer == backSigner {
			continue
		}
	}
	return victims
}

// HasSimilarAmount checks if two amounts are similar within the configured threshold
// Amount check: |A-B| / max(A,B) <= threshold
func (f *OldInBlockSandwichFinder) HasSimilarAmount(frontAmt, backAmt float64, threshold float64) (bool, float64) {
	if frontAmt <= 0 || backAmt <= 0 {
		return false, -1.0
	}

	relativeDiff := f.GetAmountRelativeDifference(frontAmt, backAmt)
	if relativeDiff < 0 {
		return false, -1.0
	}

	if relativeDiff <= 1e-6 {
		return true, 0.0 // Consider as exactly the same
	}

	return relativeDiff <= threshold/100.0, utils.FloatRound(relativeDiff, 6)
}

func (f *OldInBlockSandwichFinder) GetAmountRelativeDifference(frontAmt, backAmt float64) float64 {
	if frontAmt <= 0 || backAmt <= 0 {
		return -1.0
	}
	return math.Abs(frontAmt-backAmt) / math.Max(frontAmt, backAmt)
}

func (f *OldInBlockSandwichFinder) RecordSandwich() {
	if len(f.lastFrontTxEntries) == 0 || len(f.lastBackTxEntries) == 0 || len(f.lastVictimEntries) == 0 {
		return
	}

	sandwichId := makeSandwichID(f.Txs[f.lastFrontTxEntries[0].TxIdx].Signature, f.Txs[f.lastBackTxEntries[0].TxIdx].Signature)

	// Make SandwichTxs
	frontTxs := make([]*types.SandwichTx, 0, len(f.lastFrontTxEntries))
	for _, fe := range f.lastFrontTxEntries {
		frontTxs = append(frontTxs, f.makeSandwichTx(sandwichId, fe, "frontRun"))
	}
	transferTxs := make([]*types.SandwichTx, 0, len(f.lastTransferTxs))
	for _, te := range f.lastTransferTxs {
		transferTxs = append(transferTxs, &types.SandwichTx{
			SandwichID:          sandwichId,
			Transaction:         *te,
			Type:                "transfer",
			SandwichTxTokenInfo: types.SandwichTxTokenInfo{},
			InBundle:            false,
		})
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

	// Combine frontTxs, transferTxs when recording
	frontTxs = append(frontTxs, transferTxs...)

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
func (f *OldInBlockSandwichFinder) makeSandwichTx(sandwichId string, entry PoolEntry, kind string) *types.SandwichTx {
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
