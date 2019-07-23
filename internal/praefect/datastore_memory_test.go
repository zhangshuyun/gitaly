package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/models"
)

// TestMemoryDatastoreWhitelist verifies that the in-memory datastore will
// populate itself with the correct replication jobs and shards when initialized
// with a configuration file specifying the shard and whitelisted repositories.
func TestMemoryDatastoreWhitelist(t *testing.T) {
	mds := NewMemoryDatastore()

	mds.nodeStorages.m[1] = models.StorageNode{
		ID:          1,
		StorageName: "default",
		Address:     "tcp://default",
	}
	mds.nodeStorages.m[2] = models.StorageNode{
		ID:          2,
		StorageName: "backup-1",
		Address:     "tcp://backup-1",
	}
	mds.nodeStorages.m[3] = models.StorageNode{
		ID:          3,
		StorageName: "backup-2",
		Address:     "tcp://backup-2",
	}
	mds.shards.m["abcd1234"] = models.Shard{
		RelativePath: "abcd1234",
		Primary:      mds.nodeStorages.m[1],
		Secondaries:  []models.StorageNode{mds.nodeStorages.m[2], mds.nodeStorages.m[3]},
	}
	mds.shards.m["5678efgh"] = models.Shard{
		RelativePath: "5678efgh",
		Primary:      mds.nodeStorages.m[1],
		Secondaries:  []models.StorageNode{mds.nodeStorages.m[2], mds.nodeStorages.m[3]},
	}

	repo1 := models.Repository{
		RelativePath: "abcd1234",
		Storage:      "default",
	}

	repo2 := models.Repository{
		RelativePath: "5678efgh",
		Storage:      "default",
	}

	for _, repo := range []models.Repository{repo1, repo2} {
		jobIDs, err := mds.CreateSecondaryReplJobs(repo.RelativePath)
		require.NoError(t, err)
		require.Len(t, jobIDs, 2)
	}

	expectSecondaries := []models.StorageNode{
		models.StorageNode{ID: 2, StorageName: "backup-1", Address: "tcp://backup-1"},
		models.StorageNode{ID: 3, StorageName: "backup-2", Address: "tcp://backup-2"},
	}

	for _, repo := range []models.Repository{repo1, repo2} {
		actualSecondaries, err := mds.GetSecondaries(repo.RelativePath)
		require.NoError(t, err)
		require.ElementsMatch(t, expectSecondaries, actualSecondaries)
	}

	backup1 := mds.nodeStorages.m[2]
	backup2 := mds.nodeStorages.m[3]

	backup1ExpectedJobs := []ReplJob{
		ReplJob{
			ID:     1,
			Target: backup1.StorageName,
			Source: repo1,
			State:  JobStatePending,
		},
		ReplJob{
			ID:     3,
			Target: backup1.StorageName,
			Source: repo2,
			State:  JobStatePending,
		},
	}
	backup2ExpectedJobs := []ReplJob{
		ReplJob{
			ID:     2,
			Target: backup2.StorageName,
			Source: repo1,
			State:  JobStatePending,
		},
		ReplJob{
			ID:     4,
			Target: backup2.StorageName,
			Source: repo2,
			State:  JobStatePending,
		},
	}

	backup1ActualJobs, err := mds.GetJobs(JobStatePending|JobStateReady, backup1.StorageName, 10)
	require.NoError(t, err)
	require.Equal(t, backup1ExpectedJobs, backup1ActualJobs)

	backup2ActualJobs, err := mds.GetJobs(JobStatePending|JobStateReady, backup2.StorageName, 10)
	require.NoError(t, err)
	require.Equal(t, backup2ActualJobs, backup2ExpectedJobs)

}
