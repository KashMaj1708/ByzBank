package smallbank

import (
	"math/rand"
)

// Picker selects customer ids with optional hotspot skew.
// The hot set is striped across clusters (first hotPerCluster locals in each cluster).
type Picker struct {
	total               int
	numClusters         int
	customersPerCluster int
	hotPerCluster       int
	hotFrac             float64
	rng                 *rand.Rand
}

// NewPicker builds a skewed customer picker.
// hotAccessFraction of picks target the hot set; hotsetFraction of all customers
// form the hot set, striped evenly across numClusters.
func NewPicker(totalCustomers, numClusters, customersPerCluster int, hotsetFraction, hotAccessFraction float64, seed int64) *Picker {
	if hotsetFraction <= 0 || hotsetFraction > 1 {
		hotsetFraction = 0.1
	}
	if hotAccessFraction <= 0 || hotAccessFraction > 1 {
		hotAccessFraction = 0.9
	}
	if numClusters < 1 {
		numClusters = 1
	}
	if customersPerCluster < 1 {
		customersPerCluster = totalCustomers / numClusters
		if customersPerCluster < 1 {
			customersPerCluster = 1
		}
	}
	hot := int(float64(totalCustomers) * hotsetFraction)
	if hot < numClusters {
		hot = numClusters
	}
	hotPerCluster := hot / numClusters
	if hotPerCluster < 1 {
		hotPerCluster = 1
	}
	return &Picker{
		total:               totalCustomers,
		numClusters:         numClusters,
		customersPerCluster: customersPerCluster,
		hotPerCluster:       hotPerCluster,
		hotFrac:             hotAccessFraction,
		rng:                 rand.New(rand.NewSource(seed)),
	}
}

// Pick returns a 1-based customer id.
func (p *Picker) Pick() int {
	if p.rng.Float64() < p.hotFrac {
		cluster := p.rng.Intn(p.numClusters)
		local := 1 + p.rng.Intn(p.hotPerCluster)
		return cluster*p.customersPerCluster + local
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
