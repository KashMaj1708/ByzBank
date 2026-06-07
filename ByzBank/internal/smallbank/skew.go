package smallbank

import (
	"math/rand"
)

// Picker selects customer ids with optional hotspot skew.
type Picker struct {
	total      int
	hotCount   int
	hotFrac    float64
	rng        *rand.Rand
}

// NewPicker builds a skewed customer picker.
// hotAccessFraction of picks target the first hotsetFraction of customers.
func NewPicker(totalCustomers int, hotsetFraction, hotAccessFraction float64, seed int64) *Picker {
	if hotsetFraction <= 0 || hotsetFraction > 1 {
		hotsetFraction = 0.1
	}
	if hotAccessFraction <= 0 || hotAccessFraction > 1 {
		hotAccessFraction = 0.9
	}
	hot := int(float64(totalCustomers) * hotsetFraction)
	if hot < 1 {
		hot = 1
	}
	return &Picker{
		total:    totalCustomers,
		hotCount: hot,
		hotFrac:  hotAccessFraction,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Pick returns a 1-based customer id.
func (p *Picker) Pick() int {
	if p.rng.Float64() < p.hotFrac {
		return 1 + p.rng.Intn(p.hotCount)
	}
	return 1 + p.rng.Intn(p.total)
}

// Intn returns a non-negative int < n using the picker's RNG.
func (p *Picker) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return p.rng.Intn(n)
}
