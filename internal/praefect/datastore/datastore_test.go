package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

var (
	stor1 = models.Node{
		ID:             0,
		Address:        "tcp://address-1",
		Storage:        "praefect-storage-1",
		DefaultPrimary: true,
	}
	stor2 = models.Node{
		ID:      1,
		Address: "tcp://address-2",
		Storage: "praefect-storage-2",
	}
	proj1 = "abcd1234" // imagine this is a legit project hash
)

var (
	repo1Repository = models.Repository{
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
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor1.ID, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
	{
		desc: "insert first replication job before secondary mapped to primary",
		opFn: func(t *testing.T, ds Datastore) {
			_, err := ds.CreateReplicaReplJobs(repo1Repository.RelativePath)
			require.Error(t, err, ErrInvalidReplTarget)
		},
	},
	{
		desc: "set the primary for the repository",
		opFn: func(t *testing.T, ds Datastore) {
			err := ds.SetPrimary(repo1Repository.RelativePath, stor1.ID)
			require.NoError(t, err)
		},
	},
	{
		desc: "add a secondary replica for the repository",
		opFn: func(t *testing.T, ds Datastore) {
			err := ds.AddReplica(repo1Repository.RelativePath, stor2.ID)
			require.NoError(t, err)
		},
	},
	{
		desc: "insert first replication job after secondary mapped to primary",
		opFn: func(t *testing.T, ds Datastore) {
			ids, err := ds.CreateReplicaReplJobs(repo1Repository.RelativePath)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, ids)
		},
	},
	{
		desc: "fetch inserted replication jobs after primary mapped",
		opFn: func(t *testing.T, ds Datastore) {
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor2.ID, 10)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			expectedJob := ReplJob{
				ID: 1,
				Repository: models.Repository{
					RelativePath: repo1Repository.RelativePath,
					Primary:      stor1,
					Replicas:     []models.Node{stor2},
				},
				SourceNode: stor1,
				TargetNode: stor2,
				State:      JobStatePending,
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
			jobs, err := ds.GetJobs(JobStatePending|JobStateReady, stor1.ID, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
}

// TODO: add SQL datastore flavor
var flavors = map[string]func() Datastore{
	"in-memory-datastore": func() Datastore {
		return NewMemoryDatastore(config.Config{
			Nodes: []*models.Node{&stor1, &stor2},
		})
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
