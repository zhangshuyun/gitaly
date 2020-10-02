package config

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/limithandler"
)

// ConfigureConcurrencyLimits configures the per-repo, per RPC rate limits
func ConfigureConcurrencyLimits() {
	maxConcurrencyPerRepoPerRPC := make(map[string]int)

	for _, v := range Config.Concurrency {
		maxConcurrencyPerRepoPerRPC[v.RPC] = v.MaxPerRepo
	}

	// Set default for ReplicateRepository
	replicateRepositoryFullMethod := "/gitaly.RepositoryService/ReplicateRepository"
	if _, ok := maxConcurrencyPerRepoPerRPC[replicateRepositoryFullMethod]; !ok {
		maxConcurrencyPerRepoPerRPC[replicateRepositoryFullMethod] = 1
	}

	limithandler.SetMaxRepoConcurrency(maxConcurrencyPerRepoPerRPC)
}
