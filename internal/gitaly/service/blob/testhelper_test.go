package blob

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}

func setup(t *testing.T) (config.Cfg, *gitalypb.Repository, string, gitalypb.BlobServiceClient) {
	cfg := testcfg.Build(t)

	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	srv := testhelper.NewServer(t, nil, nil)
	t.Cleanup(srv.Stop)

	gitalypb.RegisterBlobServiceServer(srv.GrpcServer(), NewServer(cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg)))
	srv.Start(t)

	conn, err := grpc.Dial("unix://"+srv.Socket(), grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return cfg, repo, repoPath, gitalypb.NewBlobServiceClient(conn)
}
