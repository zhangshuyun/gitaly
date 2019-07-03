package praefect_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

const (
	stor1 = "default"  // usually the primary storage location
	stor2 = "backup-1" // usually the seoncary storage location
	proj1 = "abcd1234" // imagine this is a legit project hash
)

var (
	repo1Primary = praefect.Repository{
		RelativePath: proj1,
		Storage:      stor1,
	}
)

var operations = []struct {
	desc string
	opFn func(*testing.T, praefect.Datastore)
}{
	{
		desc: "query an empty datastore",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			jobs, err := ds.GetJobs(praefect.JobStatePending|praefect.JobStateReady, stor1, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
	{
		desc: "insert first replication job before secondary mapped to primary",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			_, err := ds.CreateSecondaryReplJobs(repo1Primary)
			require.Error(t, err, praefect.ErrInvalidReplTarget)
		},
	},
	{
		desc: "associate the replication job target with a primary",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			err := ds.AddSecondaries(repo1Primary, praefect.Node{Storage: stor2})
			require.NoError(t, err)
		},
	},
	{
		desc: "insert first replication job after secondary mapped to primary",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			ids, err := ds.CreateSecondaryReplJobs(repo1Primary)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, ids)
		},
	},
	{
		desc: "fetch inserted replication jobs after primary mapped",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			jobs, err := ds.GetJobs(praefect.JobStatePending|praefect.JobStateReady, stor2, 10)
			require.NoError(t, err)
			require.Len(t, jobs, 1)

			expectedJob := praefect.ReplJob{
				ID:     1,
				Source: repo1Primary,
				Target: stor2,
				State:  praefect.JobStatePending,
			}
			require.Equal(t, expectedJob, jobs[0])
		},
	},
	{
		desc: "mark replication job done",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			err := ds.UpdateReplJob(1, praefect.JobStateComplete)
			require.NoError(t, err)
		},
	},
	{
		desc: "try fetching completed replication job",
		opFn: func(t *testing.T, ds praefect.Datastore) {
			jobs, err := ds.GetJobs(praefect.JobStatePending|praefect.JobStateReady, stor1, 1)
			require.NoError(t, err)
			require.Len(t, jobs, 0)
		},
	},
}

// TODO: add SQL datastore flavor
var flavors = map[string]func() praefect.Datastore{
	"in-memory-datastore": func() praefect.Datastore {
		return praefect.NewMemoryDatastore(
			config.Config{
				Servers: []*config.GitalyServer{
					{
						Name: "default",
					},
				},
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
