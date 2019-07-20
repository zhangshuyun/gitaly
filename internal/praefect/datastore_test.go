package praefect

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

var (
	stor1 = models.StorageNode{
		ID:          1,
		Address:     "tcp://address-1",
		StorageName: "stor1",
	}
	stor2 = models.StorageNode{
		ID:          2,
		Address:     "tcp://address-2",
		StorageName: "backup-1",
	} // usually the seoncary storage location
	proj1 = "abcd1234" // imagine this is a legit project hash
)

var (
	repo1Shard = models.Shard{
		RelativePath: proj1,
	}
)

var operations = []struct {
	desc string
	opFn func(*testing.T, Datastore)
}{
	{
		desc: "query an empty datastore",
		opFn: func(t *testing.T, ds Datastore) {
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor1.StorageName, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
	{
		desc: "creating replication jobs before secondaries are added results in no jobs added",
		opFn: func(t *testing.T, ds Datastore) {
			jobIDs, err := ds.CreateSecondaryReplJobs(repo1Shard.RelativePath)
			require.NoError(t, err)
			require.Empty(t, jobIDs)
		},
	},
	{
		desc: "set the primary for the shard",
		opFn: func(t *testing.T, ds Datastore) {
			err := ds.SetPrimary(repo1Shard.RelativePath, stor1.ID)
			require.NoError(t, err)
		},
	},
	{
		desc: "associate the replication job target with a primary",
		opFn: func(t *testing.T, ds Datastore) {
			err := ds.AddSecondary(repo1Shard.RelativePath, stor2.ID)
			require.NoError(t, err)
		},
	},
	{
		desc: "insert first replication job after secondary mapped to primary",
		opFn: func(t *testing.T, ds Datastore) {
			ids, err := ds.CreateSecondaryReplJobs(repo1Shard.RelativePath)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, ids)
		},
	},
	{
		desc: "fetch inserted replication jobs after primary mapped",
		opFn: func(t *testing.T, ds Datastore) {
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor2.StorageName, 10)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			expectedJob := ReplJob{
				ID: 1,
				Source: models.Repository{
					RelativePath: repo1Shard.RelativePath,
					Storage:      stor1.StorageName,
				},
				Target: stor2.StorageName,
				State:  JobStatePending,
			}
			require.Equal(t, expectedJob, jobs[0])
		},
	},
	{
		desc: "mark replication job done",
		opFn: func(t *testing.T, ds Datastore) {
			err := ds.UpdateReplJob(1, JobStateComplete)
			require.NoError(t, err)
		},
	},
	{
		desc: "try fetching completed replication job",
		opFn: func(t *testing.T, ds Datastore) {
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor1.StorageName, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
}

// TODO: add SQL datastore flavor
var flavors = map[string]func() Datastore{
	"in-memory-datastore": func() Datastore {
		ds := NewMemoryDatastore()

		ds.shards.m[repo1Shard.RelativePath] = repo1Shard
		ds.nodeStorages.m[stor1.ID] = stor1
		ds.nodeStorages.m[stor2.ID] = stor2

		return ds
	},
}

// TestDatastoreInterface will verify that every implementation or "flavor" of
// datastore interface (in-Memory or SQL) behaves consistently given the same
// series of operations
func TestDatastoreInterface(t *testing.T) {
	for name, dsFactory := range flavors {
		t.Run(name, func(t *testing.T) {
			ds := dsFactory()
			for i, op := range operations {
				t.Logf("operation %d: %s", i+1, op.desc)
				op.opFn(t, ds)
			}
		})
	}
}
