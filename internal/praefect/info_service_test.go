package praefect

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	cfg := gconfig.Config

	tempDir := testhelper.TempDir(t)

	cfg.Storages = []gconfig.Storage{{Name: "gitaly-1"}, {Name: "gitaly-2"}, {Name: "gitaly-3"}}
	for i := range cfg.Storages {
		storagePath := filepath.Join(tempDir, cfg.Storages[i].Name)
		require.NoError(t, os.MkdirAll(storagePath, 0755))
		cfg.Storages[i].Path = storagePath
	}

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps.GetCfg(), deps.GetRubyServer(), deps.GetLocator(), deps.GetTxManager(), deps.GetGitCmdFactory()))
	}, testserver.WithDisablePraefect())

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "default",
				Nodes: []*config.Node{
					{
						Storage: "gitaly-1",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
					{
						Storage: "gitaly-2",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
					{
						Storage: "gitaly-3",
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	testRepo := gittest.CloneRepoAtStorageRoot(t, cfg.Storages[0].Path, "repo-1")
	gittest.CloneRepoAtStorageRoot(t, cfg.Storages[1].Path, "repo-1")
	gittest.CloneRepoAtStorageRoot(t, cfg.Storages[2].Path, "repo-1")

	// create a commit in the second replica so we can check that its checksum is different than the primary
	gittest.CreateCommit(t, cfg, filepath.Join(cfg.Storages[1].Path, "repo-1"), "master", nil)

	nodeManager, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
	require.NoError(t, err)
	nodeManager.Start(0, time.Hour)
	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{
		withPrimaryGetter: nodeManager,
		withConnections:   NodeSetFromNodeManager(nodeManager).Connections(),
	})
	defer cleanup()

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// CalculateChecksum through praefect will get the checksum of the primary
	repoClient := gitalypb.NewRepositoryServiceClient(cc)
	checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})
	require.NoError(t, err)

	resp, err := client.RepositoryReplicas(ctx, &gitalypb.RepositoryReplicasRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})

	require.NoError(t, err)

	require.Equal(t, checksum.Checksum, resp.Primary.Checksum)
	var checked []string
	for _, secondary := range resp.GetReplicas() {
		switch storage := secondary.GetRepository().GetStorageName(); storage {
		case conf.VirtualStorages[0].Nodes[1].Storage:
			require.NotEqual(t, checksum.Checksum, secondary.Checksum, "should not be equal since we added a commit")
			checked = append(checked, storage)
		case conf.VirtualStorages[0].Nodes[2].Storage:
			require.Equal(t, checksum.Checksum, secondary.Checksum)
			checked = append(checked, storage)
		default:
			require.FailNow(t, "unexpected storage: %q", storage)
		}
	}
	require.ElementsMatch(t, []string{conf.VirtualStorages[0].Nodes[1].Storage, conf.VirtualStorages[0].Nodes[2].Storage}, checked)
}
