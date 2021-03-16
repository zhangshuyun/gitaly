package blob

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
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

func TestWithRubySidecar(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	t.Run("testSuccessfulGetLFSPointersRequest", func(t *testing.T) { testSuccessfulGetLFSPointersRequest(t, cfg, rubySrv) })
	t.Run("testSuccessfulGetAllLFSPointersRequest", func(t *testing.T) { testSuccessfulGetAllLFSPointersRequest(t, cfg, rubySrv) })
	t.Run("testSuccessfulGetNewLFSPointersRequest", func(t *testing.T) { testSuccessfulGetNewLFSPointersRequest(t, cfg, rubySrv) })
	t.Run("testGetAllLFSPointersVerifyScope", func(t *testing.T) { testGetAllLFSPointersVerifyScope(t, cfg, rubySrv) })
}

func setup(t *testing.T) (config.Cfg, *gitalypb.Repository, string, gitalypb.BlobServiceClient) {
	cfg := testcfg.Build(t)
	repo, repoPath, client := setupWithRuby(t, cfg, nil)

	return cfg, repo, repoPath, client
}

func setupWithRuby(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) (*gitalypb.Repository, string, gitalypb.BlobServiceClient) {
	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	srv := testhelper.NewServer(t, nil, nil)
	t.Cleanup(srv.Stop)

	gitalypb.RegisterBlobServiceServer(srv.GrpcServer(), NewServer(rubySrv, cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg)))
	srv.Start(t)

	conn, err := grpc.Dial("unix://"+srv.Socket(), grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return repo, repoPath, gitalypb.NewBlobServiceClient(conn)
}
