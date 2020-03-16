package wiki

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type createWikiPageOpts struct {
	title             string
	content           []byte
	format            string
	forceContentEmpty bool
}

var (
	mockPageContent = bytes.Repeat([]byte("Mock wiki page content"), 10000)
	rubyServer      = &rubyserver.Server{}
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	tempDir, err := ioutil.TempDir("", "gitaly")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	hooks.Override = tempDir + "/hooks"
	config.Config.InternalSocketDir = tempDir + "/sock"

	if err := rubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer rubyServer.Stop()

	return m.Run()
}

func runWikiServiceServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterWikiServiceServer(srv.GrpcServer(), &server{ruby: rubyServer})
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
}

func newWikiClient(t *testing.T, serverSocketPath string) (gitalypb.WikiServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewWikiServiceClient(conn), conn
}

func writeWikiPage(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository, opts createWikiPageOpts) {
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

func updateWikiPage(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository, name string, content []byte) {
	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Update " + name),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	request := &gitalypb.WikiUpdatePageRequest{
		Repository:    wikiRepo,
		PagePath:      []byte(name),
		Title:         []byte(name),
		Format:        "markdown",
		CommitDetails: commitDetails,
		Content:       content,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.WikiUpdatePage(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(request))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)
}

func setupWikiRepo(t *testing.T) (*gitalypb.Repository, string, func()) {
	relPath := strings.Join([]string{t.Name(), "wiki-test.git"}, "-")
	storagePath := testhelper.GitlabTestStoragePath()
	wikiRepoPath := path.Join(storagePath, relPath)

	testhelper.MustRunCommand(nil, nil, "git", "init", "--bare", wikiRepoPath)

	wikiRepo := &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: relPath,
	}

	return wikiRepo, wikiRepoPath, func() { os.RemoveAll(wikiRepoPath) }
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

func createTestWikiPage(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository, opts createWikiPageOpts) *gitalypb.GitCommit {
	ctx, cancel := testhelper.Context()
	defer cancel()

	wikiRepoPath, err := helper.GetRepoPath(wikiRepo)
	require.NoError(t, err)
	writeWikiPage(t, client, wikiRepo, opts)
	head1ID := testhelper.MustRunCommand(t, nil, "git", "-C", wikiRepoPath, "show", "--format=format:%H", "--no-patch", "HEAD")
	pageCommit, err := gitlog.GetCommit(ctx, wikiRepo, string(head1ID))
	require.NoError(t, err, "look up git commit after writing a wiki page")

	return pageCommit
}

func requireWikiPagesEqual(t *testing.T, expectedPage *gitalypb.WikiPage, actualPage *gitalypb.WikiPage) {
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
