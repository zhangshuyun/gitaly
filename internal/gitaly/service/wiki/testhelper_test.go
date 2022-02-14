package wiki

import (
	"bytes"
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type createWikiPageOpts struct {
	title             string
	content           []byte
	format            string
	forceContentEmpty bool
}

var mockPageContent = bytes.Repeat([]byte("Mock wiki page content"), 10000)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestWithRubySidecar(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg, gittest.NewCommandFactory(t, cfg))
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	client, socketPath := setupWikiService(t, cfg, rubySrv)
	cfg.SocketPath = socketPath

	fs := []func(t *testing.T, cfg config.Cfg, client gitalypb.WikiServiceClient, rubySrv *rubyserver.Server){
		testSuccessfulWikiFindPageRequest,
		testSuccessfulWikiFindPageSameTitleDifferentPathRequest,
		testSuccessfulWikiFindPageRequestWithTrailers,
		testSuccessfulWikiGetAllPagesRequest,
		testWikiGetAllPagesSorting,
		testFailedWikiGetAllPagesDueToValidation,
		testSuccessfulWikiListPagesRequest,
		testWikiListPagesSorting,
		testSuccessfulWikiUpdatePageRequest,
		testFailedWikiUpdatePageDueToValidations,
		testFailedWikiUpdatePageDueToDuplicatePage,
		testSuccessfulWikiWritePageRequest,
		testFailedWikiWritePageDueToDuplicatePage,
		testFailedWikiWritePageInPathDueToDuplicatePage,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, client, rubySrv)
		})
	}
}

func setupWikiService(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server) (gitalypb.WikiServiceClient, string) {
	addr := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
		gitalypb.RegisterWikiServiceServer(srv, NewServer(deps.GetRubyServer(), deps.GetLocator()))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			cfg,
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
		))
	})
	testcfg.BuildGitalyHooks(t, cfg)
	client := newWikiClient(t, addr)
	return client, addr
}

func newWikiClient(t testing.TB, serverSocketPath string) gitalypb.WikiServiceClient {
	t.Helper()

	conn, err := grpc.Dial(serverSocketPath, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewWikiServiceClient(conn)
}

func writeWikiPage(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository, opts createWikiPageOpts) {
	t.Helper()

	var content []byte
	if len(opts.content) == 0 && !opts.forceContentEmpty {
		content = mockPageContent
	} else {
		content = opts.content
	}

	var format string
	if len(opts.format) == 0 {
		format = "markdown"
	} else {
		format = opts.format
	}

	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Add " + opts.title),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	request := &gitalypb.WikiWritePageRequest{
		Repository:    wikiRepo,
		Name:          []byte(opts.title),
		Format:        format,
		CommitDetails: commitDetails,
		Content:       content,
	}
	ctx := testhelper.Context(t)

	stream, err := client.WikiWritePage(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(request))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)
}

func setupWikiRepo(ctx context.Context, t *testing.T, cfg config.Cfg) (*gitalypb.Repository, string) {
	return gittest.CreateRepository(ctx, t, cfg)
}

func sendBytes(data []byte, chunkSize int, sender func([]byte) error) (int, error) {
	i := 0
	for ; len(data) > 0; i++ {
		n := chunkSize
		if n > len(data) {
			n = len(data)
		}

		if err := sender(data[:n]); err != nil {
			return i, err
		}
		data = data[n:]
	}

	return i, nil
}

func createTestWikiPage(t *testing.T, cfg config.Cfg, client gitalypb.WikiServiceClient, wikiRepoProto *gitalypb.Repository, wikiRepoPath string, opts createWikiPageOpts) *gitalypb.GitCommit {
	t.Helper()
	ctx := testhelper.Context(t)

	writeWikiPage(t, client, wikiRepoProto, opts)
	head1ID := gittest.Exec(t, cfg, "-C", wikiRepoPath, "show", "--format=format:%H", "--no-patch", "HEAD")

	wikiRepo := localrepo.NewTestRepo(t, cfg, wikiRepoProto)
	pageCommit, err := wikiRepo.ReadCommit(ctx, git.Revision(head1ID))
	require.NoError(t, err, "look up git commit after writing a wiki page")

	return pageCommit
}

func requireWikiPagesEqual(t *testing.T, expectedPage *gitalypb.WikiPage, actualPage *gitalypb.WikiPage) {
	t.Helper()

	// require.Equal doesn't display a proper diff when either expected/actual has a field
	// with large data (RawData in our case), so we compare file attributes and content separately.
	expectedContent := expectedPage.GetRawData()
	if expectedPage != nil {
		expectedPage.RawData = nil
		defer func() {
			expectedPage.RawData = expectedContent
		}()
	}
	actualContent := actualPage.GetRawData()
	if actualPage != nil {
		actualPage.RawData = nil
		defer func() {
			actualPage.RawData = actualContent
		}()
	}

	require.Equal(t, expectedPage, actualPage, "mismatched page attributes")
	if expectedPage != nil {
		require.Equal(t, expectedContent, actualContent, "mismatched page content")
	}
}
