package types

// Token amount maps token mint address to its amount
// token => amount
type TokenAmount map[string]float64

type TokenInTx struct {
	InFrontTx float64
	InBackTx  float64
}
