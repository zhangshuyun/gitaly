package praefect

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gitalyconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRepoService_WriteRef_Success(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					{
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					{
						Storage: "praefect-internal-2",
					},
					{
						Storage: "praefect-internal-3",
					},
				},
			},
		},
	}

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	newRev, _ := testhelper.CreateCommitOnNewBranch(t, testRepoPath)

	cleanupTempStoragePaths := createTempStoragePaths(t, conf.VirtualStorages[0].Nodes)
	defer cleanupTempStoragePaths()

	for _, node := range conf.VirtualStorages[0].Nodes {
		cloneRepoAtStorage(t, testRepo, node.Storage)
	}

	cc, components, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	nodeMgr := components.NodeManager

	shard, err := nodeMgr.GetShard("default")
	require.NoError(t, err)

	primary, err := shard.GetPrimary()
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ref := "refs/heads/master"
	branch, err := gitalypb.NewRefServiceClient(primary.GetConnection()).FindBranch(ctx, &gitalypb.FindBranchRequest{
		Repository: &gitalypb.Repository{
			StorageName:  primary.GetStorage(),
			RelativePath: testRepo.GetRelativePath(),
		},
		Name: []byte(ref),
	})
	require.NoError(t, err)

	oldRev := branch.Branch.TargetCommit.Id

	_, err = gitalypb.NewRepositoryServiceClient(cc).WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository:  testRepo,
		Ref:         []byte(ref),
		Revision:    []byte(newRev),
		OldRevision: []byte(oldRev),
		Force:       false,
	})
	require.NoError(t, err)

	secondaries, err := shard.GetSecondaries()
	require.NoError(t, err)

	for _, secondary := range secondaries {
		branch, err = gitalypb.NewRefServiceClient(secondary.GetConnection()).FindBranch(ctx, &gitalypb.FindBranchRequest{
			Repository: &gitalypb.Repository{
				StorageName:  secondary.GetStorage(),
				RelativePath: testRepo.GetRelativePath(),
			},
			Name: []byte(ref),
		})
		require.NoError(t, err)
		require.Equal(t, newRev, branch.GetBranch().TargetCommit.Id)
	}
}

func TestRepoService_WriteRef_Replication(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					{
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					{
						Storage: "praefect-internal-2",
					},
					{
						Storage: "praefect-internal-3",
					},
				},
			},
		},
	}

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupTempStoragePaths := createTempStoragePaths(t, conf.VirtualStorages[0].Nodes)
	defer cleanupTempStoragePaths()

	for _, node := range conf.VirtualStorages[0].Nodes {
		cloneRepoAtStorage(t, testRepo, node.Storage)
	}

	primaryRepoPath, err := helper.GetRepoPath(&gitalypb.Repository{
		StorageName:  conf.VirtualStorages[0].Nodes[0].Storage,
		RelativePath: testRepo.GetRelativePath(),
	})
	require.NoError(t, err)

	newRev, _ := testhelper.CreateCommitOnNewBranch(t, primaryRepoPath)

	cc, components, cleanupServers := runPraefectServerWithGitaly(t, conf)
	defer cleanupServers()

	nodeMgr := components.NodeManager

	shard, err := nodeMgr.GetShard("default")
	require.NoError(t, err)

	primary, err := shard.GetPrimary()
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ref := "refs/heads/master"
	branch, err := gitalypb.NewRefServiceClient(primary.GetConnection()).FindBranch(ctx, &gitalypb.FindBranchRequest{
		Repository: &gitalypb.Repository{
			StorageName:  primary.GetStorage(),
			RelativePath: testRepo.GetRelativePath(),
		},
		Name: []byte(ref),
	})
	require.NoError(t, err)

	oldRev := branch.Branch.TargetCommit.Id

	_, err = gitalypb.NewRepositoryServiceClient(cc).WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository:  testRepo,
		Ref:         []byte(ref),
		Revision:    []byte(newRev),
		OldRevision: []byte(oldRev),
		Force:       false,
	})
	require.NoError(t, err)

	secondaries, err := shard.GetSecondaries()
	require.NoError(t, err)

	for _, secondary := range secondaries {
		branch, err = gitalypb.NewRefServiceClient(secondary.GetConnection()).FindBranch(ctx, &gitalypb.FindBranchRequest{
			Repository: &gitalypb.Repository{
				StorageName:  secondary.GetStorage(),
				RelativePath: testRepo.GetRelativePath(),
			},
			Name: []byte(ref),
		})
		require.NoError(t, err)
		require.NotEqual(t, newRev, branch.GetBranch().TargetCommit.Id, "write ref should have failed on the secondaries")
	}

	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()

Test:
	for {
		select {
		case <-timer.C:
			t.Fatal("time limit expired and jobs have not been completed")
		default:
			secondary1Jobs, err := components.Datastore.GetJobs(
				datastore.JobStateComplete|datastore.JobStateInProgress|datastore.JobStateDead|datastore.JobStateReady|datastore.JobStatePending,
				conf.VirtualStorages[0].Nodes[1].Storage, 1)
			require.NoError(t, err)
			secondary2Jobs, err := components.Datastore.GetJobs(
				datastore.JobStateComplete|datastore.JobStateInProgress|datastore.JobStateDead|datastore.JobStateReady|datastore.JobStatePending,
				conf.VirtualStorages[0].Nodes[2].Storage, 1)
			require.NoError(t, err)
			if len(secondary1Jobs) == 0 && len(secondary2Jobs) == 0 {
				break Test
			}
			<-time.Tick(100 * time.Millisecond)
		}
	}

	for _, secondary := range secondaries {
		branch, err = gitalypb.NewRefServiceClient(secondary.GetConnection()).FindBranch(ctx, &gitalypb.FindBranchRequest{
			Repository: &gitalypb.Repository{
				StorageName:  secondary.GetStorage(),
				RelativePath: testRepo.GetRelativePath(),
			},
			Name: []byte(ref),
		})
		require.NoError(t, err)
		require.Equal(t, newRev, branch.GetBranch().TargetCommit.Id, "write ref should have failed on the secondaries")
	}
}

func createTempStoragePaths(t *testing.T, nodes []*models.Node) func() {
	oldStorages := gitalyconfig.Config.Storages

	var tempDirCleanups []func() error
	for _, node := range nodes {
		tempPath, cleanup := testhelper.TempDir(t, node.Storage)
		tempDirCleanups = append(tempDirCleanups, cleanup)

		gitalyconfig.Config.Storages = append(gitalyconfig.Config.Storages, gitalyconfig.Storage{
			Name: node.Storage,
			Path: tempPath,
		})
	}

	return func() {
		gitalyconfig.Config.Storages = oldStorages
		for _, tempDirCleanup := range tempDirCleanups {
			tempDirCleanup()
		}
	}
}
