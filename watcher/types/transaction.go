package types

import (
	"time"
	"watcher/utils"

	MapSet "github.com/deckarep/golang-set/v2"
)

type Block struct {
	Slot        uint64
	BlockHeight uint64
	Timestamp   time.Time
	Txs         []*Transaction
	// RelatedAddrs *utils.UnionFind[string]
}

type Blocks []*Block

type Transaction struct {
	Slot      uint64 `json:"slot"`
	Position  int
	Timestamp time.Time `json:"timestamp"`
	IsFailed  bool
	IsVote    bool

	// The identifier of this transaction, which is the first signature in Signatures field.
	// A 64 bytes Ed25519 signature, encoded as a base-58 string.
	Signature string
	// The account address that signed the transaction and paid the fee, which is thte first account in the AccountKeys array.
	Signer string
	// All accounts that signed the transaction and paid the fee
	AccountKeys []string `json:"accountKeys"`
	Programs    []string

	// In Solana, for each token/WSOL, each user has an Associated Token Account, ATA, which is controlled by the address of the hold (owner). An ATA is derived from the owner's address and the token mint address.
	//
	// owner -> token/SOL -> []{ata, delta}
	// Record the balance changes of each owner for each token/SOL in this transaction.
	// A token is labeled by its mint address, e.g.
	// USDC: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1vl,
	// WSOL: So11111111111111111111111111111111111111112,
	// SOL is labeled by "SOL"
	// A owner may have multiple ATAs for the same token
	OwnerBalanceChanges map[string]map[string]AtaAmounts `ch:"ownerBalanceChanges" json:"ownerBalanceChanges"`
	// owner -> token/SOL -> pre balance
	// Record the pre-transaction balance of each owner for each token/SOL in this transaction.
	OwnerPreBalances map[string]map[string]float64 `ch:"ownerPreBalances" json:"ownerPreBalances"`
	// ata -> owner address
	// Record the owner address of each ATA involved in this transaction.
	AtaOwner map[string]string `ch:"ataOwner" json:"ataOwner"`
	// RelatedTokens records all tokens involved in this transaction
	RelatedTokens MapSet.Set[string] `ch:"relatedTokens" json:"relatedTokens"`
	// RelatedPools records all pools involved in this transaction
	// A pool is a Solana address in frontTx that has at least 1 income token and 1 expense token throughout this tx, excluding the signer itself, e.g., WSOL/USDC pool
	// RecordPoolsInfo records the detailed token changes of each related pool
	RelatedPools     MapSet.Set[string]    `ch:"relatedPools" json:"relatedPools"`
	RelatedPoolsInfo map[string]PoolAmount // pool address -> {fromToken, fromAmt, toToken, toAmt}
}

type Transactions []*Transaction

// PostprocessForFindSandwich performs post-processing on the transaction data to prepare it for sandwich detection.
// It extracts and organizes relevant information such as RelatedTokens and RelatedPools.
func (tx *Transaction) PostprocessForFindSandwich() {
	if tx.IsFailed || tx.IsVote {
		return // Skip failed or vote transactions
	}

	// Combine SOL and WSOL
	// Other tokens like jitoSOL, bSOL shold not be combined
	for owner, tokenChanges := range tx.OwnerBalanceChanges {
		solChanges, hasSOL := tokenChanges[utils.SOL]
		wsolChanges, hasWSOL := tokenChanges[utils.WSOL]
		if !hasWSOL {
			continue
		}

		if hasSOL {
			// Add all WSOL changes to SOL changes
			for i := range wsolChanges.AtaAddress {
				solChanges.AddAtaAmount(wsolChanges.AtaAddress[i], wsolChanges.Amount[i])
			}
			tokenChanges[utils.SOL] = solChanges
			delete(tokenChanges, utils.WSOL)
		} else {
			// Rename WSOL to SOL
			tokenChanges[utils.SOL] = wsolChanges
			delete(tokenChanges, utils.WSOL)
		}
		tx.OwnerBalanceChanges[owner] = tokenChanges
	}
	for owner, preBalances := range tx.OwnerPreBalances {
		solPreBalance, hasSOL := preBalances[utils.SOL]
		wsolPreBalance, hasWSOL := preBalances[utils.WSOL]
		if !hasWSOL {
			continue
		}

		if hasSOL {
			preBalances[utils.SOL] = solPreBalance + wsolPreBalance
			delete(preBalances, utils.WSOL)
		} else {
			preBalances[utils.SOL] = wsolPreBalance
			delete(preBalances, utils.WSOL)
		}
		tx.OwnerPreBalances[owner] = preBalances
	}

	// Useful things for sandwich detection
	// Collect potentially related tokens
	relatedTokens := MapSet.NewSet[string]()
	for _, tokenChanges := range tx.OwnerBalanceChanges {
		for token := range tokenChanges {
			relatedTokens.Add(token)
		}
	}
	tx.RelatedTokens = relatedTokens
	// Collect potentially related pools (heuristic: any owner with only 1 income token and 1 expense token throughout this tx)
	relatedPools := MapSet.NewSet[string]()
	relatedPoolsInfo := make(map[string]PoolAmount)
	for owner, tokenChanges := range tx.OwnerBalanceChanges {
		if owner == tx.Signer {
			continue // skip signer
		}

		income := 0
		expense := 0
		poolInfo := PoolAmount{}
		for token, change := range tokenChanges {
			if change.GetTotalAmount() < 0 {
				expense++
				poolInfo.ExpenseToken = token
				poolInfo.ExpenseAmt = change.GetTotalAmount()
			} else if change.GetTotalAmount() > 0 {
				income++
				poolInfo.IncomeToken = token
				poolInfo.IncomeAmt = change.GetTotalAmount()
			}
		}

		// A pool has exact 1 income token and 1 expense token
		if income == 1 && expense == 1 {
			relatedPools.Add(owner)
			relatedPoolsInfo[owner] = poolInfo
		}
	}
	tx.RelatedPools = relatedPools
	tx.RelatedPoolsInfo = relatedPoolsInfo
}

// PoolAmount represents the token changes of a pool in a transaction.
// A pool has exactly one IncomeToken with positive amount and one OutcomeToken with negative amount.
type PoolAmount struct {
	IncomeToken  string
	ExpenseToken string
	IncomeAmt    float64
	ExpenseAmt   float64
}

// AtaAmounts represents pairs of ATA addresses and their corresponding amounts for a specific token.
type AtaAmounts struct {
	AtaAddress  []string
	Amount      []float64
	TotalAmount float64
}

func NewAtaAmounts() AtaAmounts {
	return AtaAmounts{
		AtaAddress:  make([]string, 0),
		Amount:      make([]float64, 0),
		TotalAmount: 0,
	}
}

func (p *AtaAmounts) AddAtaAmount(ata string, amount float64) {
	p.AtaAddress = append(p.AtaAddress, ata)
	p.Amount = append(p.Amount, amount)
	p.TotalAmount += amount
}

func (p *AtaAmounts) GetTotalAmount() float64 {
	return p.TotalAmount
}
