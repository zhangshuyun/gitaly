package praefect_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

// TestMemoryDatastoreWhitelist verifies that the in-memory datastore will
// populate itself with the correct replication jobs and shards when initialized
// with a configuration file specifying the shard and whitelisted repositories.
func TestMemoryDatastoreWhitelist(t *testing.T) {
	cfg := config.Config{
		Servers: []*config.GitalyServer{
			{
				Name: "default",
			},
			{
				Name: "backup-1",
			},
			{
				Name: "backup-2",
			},
		},
		Whitelist: []string{
			"abcd1234",
			"5678efgh",
		},
	}

	mds := praefect.NewMemoryDatastore(cfg)

	repo1 := praefect.Repository{
		RelativePath: cfg.Whitelist[0],
		Storage:      cfg.Servers[0].Name,
	}
	repo2 := praefect.Repository{
		RelativePath: cfg.Whitelist[1],
		Storage:      cfg.Servers[0].Name,
	}

	expectSecondaries := []praefect.Node{
		praefect.Node{Storage: cfg.Servers[1].Name},
		praefect.Node{Storage: cfg.Servers[2].Name},
	}

	for _, repo := range []praefect.Repository{repo1, repo2} {
		actualSecondaries, err := mds.GetSecondaries(repo)
		require.NoError(t, err)
		require.ElementsMatch(t, actualSecondaries, expectSecondaries)
	}

	backup1 := cfg.Servers[1]
	backup2 := cfg.Servers[2]

	backup1ExpectedJobs := []praefect.ReplJob{
		praefect.ReplJob{
			ID:     1,
			Target: backup1.Name,
			Source: repo1,
			State:  praefect.JobStateReady,
		},
		praefect.ReplJob{
			ID:     3,
			Target: backup1.Name,
			Source: repo2,
			State:  praefect.JobStateReady,
		},
	}
	backup2ExpectedJobs := []praefect.ReplJob{
		praefect.ReplJob{
			ID:     2,
			Target: backup2.Name,
			Source: repo1,
			State:  praefect.JobStateReady,
		},
		praefect.ReplJob{
			ID:     4,
			Target: backup2.Name,
			Source: repo2,
			State:  praefect.JobStateReady,
		},
	}

	backup1ActualJobs, err := mds.GetJobs(praefect.JobStatePending|praefect.JobStateReady, backup1.Name, 10)
	require.NoError(t, err)
	require.Equal(t, backup1ExpectedJobs, backup1ActualJobs)

	backup2ActualJobs, err := mds.GetJobs(praefect.JobStatePending|praefect.JobStateReady, backup2.Name, 10)
	require.NoError(t, err)
	require.Equal(t, backup2ActualJobs, backup2ExpectedJobs)

}
