package praefect

// HealthChecker manages information of locally healthy nodes.
type HealthChecker interface {
	// HealthyNodes gets a list of healthy storages by their virtual storage.
	HealthyNodes() map[string][]string
}

// StaticHealthChecker returns the nodes as always healthy.
type StaticHealthChecker map[string][]string

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (healthyNodes StaticHealthChecker) HealthyNodes() map[string][]string {
	return healthyNodes
}
