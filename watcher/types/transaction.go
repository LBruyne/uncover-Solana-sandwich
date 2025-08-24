package types

// type Block struct {
// 	Slot      uint64
// 	Timestamp time.Time
// 	Txs       []*Transaction
// }

// type Transaction struct {
// 	Sandwich
// 	AtaOwner      map[string]string             `ch:"ataOwner" json:"ataOwner"`
// 	BalanceChange map[string]map[string]float64 `ch:"balanceChange" json:"balanceChange"`
// 	Position      int
// 	Signers       []string
// }

// type Bundle []*Transaction

// type Transactions []*Transaction

// func (tx *Transaction) GetPotentialPools() *utils.Set[string] {

// 	pools := utils.NewSet[string]()
// 	bc := tx.GetOwnerBalanceChange(true)
// 	for account, tokenAmount := range bc {
// 		tokenAmount := TokenAmount(tokenAmount)
// 		if slices.Contains(tx.Signers, account) {
// 			continue
// 		}
// 		if tokenAmount.GetIncomeTokens().Len() == 1 && tokenAmount.GetExpenseTokens().Len() == 1 {
// 			pools.Add(account)
// 		}
// 	}
// 	return pools
// }

// func (tx *Transaction) GetTokens() *utils.Set[string] {
// 	tokens := utils.NewSet[string]()
// 	for _, tokenAmount := range tx.BalanceChange {
// 		for token, _ := range tokenAmount {
// 			tokens.Add(token)
// 		}
// 	}
// 	return tokens
// }

// // Tranform ATA => token => amount
// // to owner => token => amount
// func (tx *Transaction) GetOwnerBalanceChange(filterWSOL bool) map[string]map[string]float64 {

// 	bc := make(map[string]map[string]float64)

// 	for ata, tokenAmount := range tx.BalanceChange {
// 		var owner string
// 		if ownerMapped, exist := tx.AtaOwner[ata]; exist {
// 			owner = ownerMapped
// 		} else {
// 			owner = ata
// 		}

// 		if _, exist := bc[owner]; !exist {
// 			bc[owner] = make(map[string]float64)
// 		}
// 		for token, amount := range tokenAmount {
// 			if token == labels.WSOL {
// 				continue
// 			}
// 			bc[owner][token] = amount
// 		}
// 	}
// 	return bc
// }

// func FilterPrograms(pros []string) []string {

// 	programs := utils.NewSet[string]()

// 	for _, p := range pros {
// 		if labels.IsBuiltInPrograms(p) {
// 			continue
// 		}
// 		programs.Add(p)
// 	}

// 	return programs.ToSlice()
// }
