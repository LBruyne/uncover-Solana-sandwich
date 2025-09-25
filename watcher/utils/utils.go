package utils

// Units
const (
	SOL_UNIT = 1e9 // 1 SOL = 10^9 lamports

	EPSILON = 1e-3 // Infinite small value for float comparison
)

func HasString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// FloatRound rounds a float64 to a specified number of decimal places.
// e.g. FloatRound(3.14159, 2) => 3.14
func FloatRound(x float64, precision int) float64 {
	pow := 1.0
	for i := 0; i < precision; i++ {
		pow *= 10
	}
	return float64(int(x*pow+0.5)) / pow
}
