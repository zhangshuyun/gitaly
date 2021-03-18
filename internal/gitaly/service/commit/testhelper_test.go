package commit

import (
	"io"
	"net"
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/linguist"
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

// setupCommitService makes a basic configuration and starts the service with the client.
func setupCommitService(t testing.TB) (config.Cfg, gitalypb.CommitServiceClient) {
	cfg, _, _, client := setupCommitServiceCreateRepo(t, func(tb testing.TB, cfg config.Cfg) (*gitalypb.Repository, string, testhelper.Cleanup) {
		return nil, "", func() {}
	})
	return cfg, client
}

// setupCommitServiceWithRepo makes a basic configuration, creates a test repository and starts the service with the client.
func setupCommitServiceWithRepo(
	t testing.TB, bare bool,
) (config.Cfg, *gitalypb.Repository, string, gitalypb.CommitServiceClient) {
	return setupCommitServiceCreateRepo(t, func(tb testing.TB, cfg config.Cfg) (*gitalypb.Repository, string, testhelper.Cleanup) {
		if bare {
			return gittest.CloneRepoAtStorage(tb, cfg.Storages[0], t.Name())
		}
		return gittest.CloneRepoWithWorktreeAtStorage(tb, cfg.Storages[0])
	})
}

func setupCommitServiceCreateRepo(
	t testing.TB,
	createRepo func(testing.TB, config.Cfg) (*gitalypb.Repository, string, testhelper.Cleanup),
) (config.Cfg, *gitalypb.Repository, string, gitalypb.CommitServiceClient) {
	cfg := testcfg.Build(t)

	repo, repoPath, cleanup := createRepo(t, cfg)
	t.Cleanup(cleanup)

	serverSocketPath := startTestServices(t, cfg)

	client := newCommitServiceClient(t, serverSocketPath)

	return cfg, repo, repoPath, client
}

func startTestServices(t testing.TB, cfg config.Cfg) string {
	t.Helper()

	server := testhelper.NewTestGrpcServer(t, nil, nil)
	t.Cleanup(server.Stop)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	ling, err := linguist.New(cfg)
	require.NoError(t, err)

	gitalypb.RegisterCommitServiceServer(server, NewServer(cfg, config.NewLocator(cfg), git.NewExecCommandFactory(cfg), ling))

	go server.Serve(listener)
	return "unix://" + serverSocketPath
}

func newCommitServiceClient(t testing.TB, serviceSocketPath string) gitalypb.CommitServiceClient {
	t.Helper()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serviceSocketPath, connOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return gitalypb.NewCommitServiceClient(conn)
}

func dummyCommitAuthor(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad+gitlab-test@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0200"),
	}
}

type gitCommitsGetter interface {
	GetCommits() []*gitalypb.GitCommit
}

func getAllCommits(t testing.TB, getter func() (gitCommitsGetter, error)) []*gitalypb.GitCommit {
	t.Helper()

	var commits []*gitalypb.GitCommit
	for {
		resp, err := getter()
		if err == io.EOF {
			return commits
		}
		require.NoError(t, err)

		commits = append(commits, resp.GetCommits()...)
	}
}
