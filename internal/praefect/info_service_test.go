package praefect

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gconfig "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "praefect-internal-1",
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
		Failover: config.Failover{Enabled: true},
	}

	defer func(storages []gconfig.Storage) { gconfig.Config.Storages = storages }(gconfig.Config.Storages)

	tempDir, cleanupTempDir := testhelper.TempDir(t)
	defer cleanupTempDir()

	for _, node := range conf.VirtualStorages[0].Nodes {
		storagePath := filepath.Join(tempDir, node.Storage)
		require.NoError(t, os.MkdirAll(storagePath, 0755))
		gconfig.Config.Storages = append(gconfig.Config.Storages, gconfig.Storage{
			Name: node.Storage,
			Path: storagePath,
		})
	}

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	testRepoPrimary, _, cleanup := cloneRepoAtStorage(t, testRepo, conf.VirtualStorages[0].Nodes[0].Storage)
	defer cleanup()

	_, _, cleanup = cloneRepoAtStorage(t, testRepo, conf.VirtualStorages[0].Nodes[1].Storage)
	defer cleanup()
	_, testRepoSecondary2Path, cleanup := cloneRepoAtStorage(t, testRepo, conf.VirtualStorages[0].Nodes[2].Storage)
	defer cleanup()

	// create a commit in the second replica so we can check that its checksum is different than the primary
	testhelper.CreateCommit(t, testRepoSecondary2Path, "master", nil)

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// CalculateChecksum through praefect will get the checksum of the primary
	repoClient := gitalypb.NewRepositoryServiceClient(cc)
	checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepoPrimary.GetRelativePath(),
		},
	})
	require.NoError(t, err)

	resp, err := client.RepositoryReplicas(ctx, &gitalypb.RepositoryReplicasRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepoPrimary.GetRelativePath(),
		},
	})

	require.NoError(t, err)

	require.Equal(t, checksum.Checksum, resp.Primary.Checksum)
	var checked []string
	for _, secondary := range resp.GetReplicas() {
		switch storage := secondary.GetRepository().GetStorageName(); storage {
		case conf.VirtualStorages[0].Nodes[1].Storage:
			require.Equal(t, checksum.Checksum, secondary.Checksum)
			checked = append(checked, storage)
		case conf.VirtualStorages[0].Nodes[2].Storage:
			require.NotEqual(t, checksum.Checksum, secondary.Checksum, "should not be equal since we added a commit")
			checked = append(checked, storage)
		default:
			require.FailNow(t, "unexpected storage: %q", storage)
		}
	}
	require.ElementsMatch(t, []string{conf.VirtualStorages[0].Nodes[1].Storage, conf.VirtualStorages[0].Nodes[2].Storage}, checked)
}
