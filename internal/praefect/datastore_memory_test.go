package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

// TestMemoryDatastoreWhitelist verifies that the in-memory datastore will
// populate itself with the correct replication jobs and shards when initialized
// with a configuration file specifying the shard and whitelisted repositories.
func TestMemoryDatastoreWhitelist(t *testing.T) {
	repo1 := models.Repository{
		RelativePath: "abcd1234",
		Storage:      "default",
	}

	repo2 := models.Repository{
		RelativePath: "5678efgh",
		Storage:      "default",
	}
	mds := NewMemoryDatastore()

	mds.storageNodes.m[1] = models.StorageNode{
		ID:      1,
		Address: "tcp://default",
		Storage: "praefect-internal-1",
	}
	mds.storageNodes.m[2] = models.StorageNode{
		ID:      2,
		Address: "tcp://backup-1",
		Storage: "praefect-internal-2",
	}
	mds.storageNodes.m[3] = models.StorageNode{
		ID:      3,
		Address: "tcp://backup-2",
		Storage: "praefect-internal-3",
	}
	mds.shards.m[repo1.Storage+repo1.RelativePath] = models.Shard{
		Storage:      repo1.Storage,
		RelativePath: repo1.RelativePath,
		Primary:      mds.storageNodes.m[1],
		Secondaries:  []models.StorageNode{mds.storageNodes.m[2], mds.storageNodes.m[3]},
	}
	mds.shards.m[repo2.Storage+repo2.RelativePath] = models.Shard{
		Storage:      repo2.Storage,
		RelativePath: repo2.RelativePath,
		Primary:      mds.storageNodes.m[1],
		Secondaries:  []models.StorageNode{mds.storageNodes.m[2], mds.storageNodes.m[3]},
	}

	for _, repo := range []models.Repository{repo1, repo2} {
		jobIDs, err := mds.CreateSecondaryReplJobs(repo.Storage, repo.RelativePath)
		require.NoError(t, err)
		require.Len(t, jobIDs, 2)
	}

	expectSecondaries := []models.StorageNode{
		mds.storageNodes.m[2],
		mds.storageNodes.m[3],
	}

	for _, repo := range []models.Repository{repo1, repo2} {
		actualSecondaries, err := mds.GetSecondaries(repo.Storage, repo.RelativePath)
		require.NoError(t, err)
		require.ElementsMatch(t, expectSecondaries, actualSecondaries)
	}

	backup1 := mds.storageNodes.m[2]
	backup2 := mds.storageNodes.m[3]

	backup1ExpectedJobs := []ReplJob{
		ReplJob{
			ID:           1,
			TargetNodeID: backup1.ID,
			Source:       models.Repository{RelativePath: repo1.RelativePath},
			State:        JobStatePending,
		},
		ReplJob{
			ID:           3,
			TargetNodeID: backup1.ID,
			Source:       models.Repository{RelativePath: repo2.RelativePath},
			State:        JobStatePending,
		},
	}
	backup2ExpectedJobs := []ReplJob{
		ReplJob{
			ID:           2,
			TargetNodeID: backup2.ID,
			Source:       models.Repository{RelativePath: repo1.RelativePath},
			State:        JobStatePending,
		},
		ReplJob{
			ID:           4,
			TargetNodeID: backup2.ID,
			Source:       models.Repository{RelativePath: repo2.RelativePath},
			State:        JobStatePending,
		},
	}

	backup1ActualJobs, err := mds.GetJobs(JobStatePending|JobStateReady, backup1.ID, 10)
	require.NoError(t, err)
	require.Equal(t, backup1ExpectedJobs, backup1ActualJobs)

	backup2ActualJobs, err := mds.GetJobs(JobStatePending|JobStateReady, backup2.ID, 10)
	require.NoError(t, err)
	require.Equal(t, backup2ActualJobs, backup2ExpectedJobs)

}
