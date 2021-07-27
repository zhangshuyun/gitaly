package wiki

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"runtime"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
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
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	tempDir, err := ioutil.TempDir("", "gitaly")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(tempDir)

	hooks.Override = tempDir + "/hooks"

	return m.Run()
}

func TestWithRubySidecar(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	fs := []func(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server){
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
		testSuccessfulWikiWritePageRequest,
		testFailedWikiWritePageDueToDuplicatePage,
		testFailedWikiWritePageInPathDueToDuplicatePage,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, rubySrv)
		})
	}
}

func setupWikiService(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server) gitalypb.WikiServiceClient {
	addr := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
		gitalypb.RegisterWikiServiceServer(srv, NewServer(deps.GetRubyServer(), deps.GetLocator()))
	})
	client := newWikiClient(t, addr)
	return client
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

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.WikiWritePage(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(request))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)
}

func setupWikiRepo(t *testing.T, cfg config.Cfg) (*gitalypb.Repository, string, func()) {
	return gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
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

	ctx, cancel := testhelper.Context()
	defer cancel()

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
