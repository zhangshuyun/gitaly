// Package advisorylock contains the lock IDs of all advisory locks used
// in Praefect.
package advisorylock

const (
	// Reconcile is an advisory lock that must be acquired for each reconciliation run.
	Reconcile = 1
)
