package wiki

import (
	"bytes"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type createWikiPageOpts struct {
	title   string
	content []byte
	format  string
}

var (
	mockPageContent = bytes.Repeat([]byte("Mock wiki page content"), 10000)
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

var rubyServer *rubyserver.Server

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	var err error
	testhelper.ConfigureRuby()
	rubyServer, err = rubyserver.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer rubyServer.Stop()

	return m.Run()
}

func runWikiServiceServer(t *testing.T) (*grpc.Server, string) {
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterWikiServiceServer(grpcServer, &server{rubyServer})
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, serverSocketPath
}

func newWikiClient(t *testing.T, serverSocketPath string) (gitalypb.WikiServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewWikiServiceClient(conn), conn
}

func writeWikiPage(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository, opts createWikiPageOpts) {
	var content []byte
	if len(opts.content) == 0 {
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
