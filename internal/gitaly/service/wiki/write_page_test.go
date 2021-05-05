package wiki

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func testSuccessfulWikiWritePageRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	wikiRepoProto, wikiRepoPath, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()
	wikiRepo := localrepo.NewTestRepo(t, cfg, wikiRepoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := setupWikiService(t, cfg, rubySrv)

	authorID := int32(1)
	authorUserName := []byte("ahmad")
	authorName := []byte("Ahmad Sherif")
	authorEmail := []byte("ahmad@gitlab.com")
	message := []byte("Add installation instructions")

	testCases := []struct {
		desc       string
		req        *gitalypb.WikiWritePageRequest
		gollumPath string
		content    []byte
	}{
		{
			desc: "with user id and username",
			req: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepoProto,
				Name:       []byte("Instálling Gitaly"),
				Format:     "markdown",
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     authorName,
					Email:    authorEmail,
					Message:  message,
					UserId:   authorID,
					UserName: authorUserName,
				},
			},
			gollumPath: "Instálling-Gitaly.md",
			content:    bytes.Repeat([]byte("Mock wiki page content"), 10000),
		},
		{
			desc: "without user id and username", // deprecate in gitlab 11.0 https://gitlab.com/gitlab-org/gitaly/issues/1154
			req: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepoProto,
				Name:       []byte("Instálling Gitaly 2"),
				Format:     "markdown",
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     authorName,
					Email:    authorEmail,
					Message:  message,
					UserId:   authorID,
					UserName: authorUserName,
				},
			},
			gollumPath: "Instálling-Gitaly-2.md",
			content:    bytes.Repeat([]byte("Mock wiki page content 2"), 10000),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.WikiWritePage(ctx)
			require.NoError(t, err)

			request := tc.req
			nSends, err := sendBytes(tc.content, 1000, func(p []byte) error {
				request.Content = p

				if err := stream.Send(request); err != nil {
					return err
				}

				// Use a new response so we don't send other fields (Repository, ...) over and over
				request = &gitalypb.WikiWritePageRequest{}

				return nil
			})

			require.NoError(t, err)
			require.True(t, nSends > 1, "should have sent more than one message")

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			require.Empty(t, resp.DuplicateError, "DuplicateError must be empty")

			headID := testhelper.MustRunCommand(t, nil, "git", "-C", wikiRepoPath, "show", "--format=format:%H", "--no-patch", "HEAD")
			commit, err := wikiRepo.ReadCommit(ctx, git.Revision(headID))
			require.NoError(t, err, "look up git commit after writing a wiki page")

			require.Equal(t, authorName, commit.Author.Name, "author name mismatched")
			require.Equal(t, authorEmail, commit.Author.Email, "author email mismatched")
			require.Equal(t, message, commit.Subject, "message mismatched")

			pageContent := testhelper.MustRunCommand(t, nil, "git", "-C", wikiRepoPath, "cat-file", "blob", "HEAD:"+tc.gollumPath)
			require.Equal(t, tc.content, pageContent, "mismatched content")
		})
	}
}

func testFailedWikiWritePageDueToDuplicatePage(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	client := setupWikiService(t, cfg, rubySrv)

	pageName := "Installing Gitaly"
	content := []byte("Mock wiki page content")
	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Add " + pageName),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	writeWikiPage(t, client, wikiRepo, createWikiPageOpts{title: pageName, content: content})

	request := &gitalypb.WikiWritePageRequest{
		Repository:    wikiRepo,
		Name:          []byte(pageName),
		Format:        "markdown",
		CommitDetails: commitDetails,
		Content:       content,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.WikiWritePage(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(request))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)

	expectedResponse := &gitalypb.WikiWritePageResponse{DuplicateError: []byte("Cannot write //Installing-Gitaly.md, found //Installing-Gitaly.md.")}
	testhelper.ProtoEqual(t, expectedResponse, response)
}

func testFailedWikiWritePageInPathDueToDuplicatePage(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	client := setupWikiService(t, cfg, rubySrv)

	pageName := "foo/Installing Gitaly"
	content := []byte("Mock wiki page content")
	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Add " + pageName),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	writeWikiPage(t, client, wikiRepo, createWikiPageOpts{title: pageName, content: content})

	request := &gitalypb.WikiWritePageRequest{
		Repository:    wikiRepo,
		Name:          []byte(pageName),
		Format:        "markdown",
		CommitDetails: commitDetails,
		Content:       content,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.WikiWritePage(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(request))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)

	expectedResponse := &gitalypb.WikiWritePageResponse{DuplicateError: []byte("Cannot write foo/Installing-Gitaly.md, found foo/Installing-Gitaly.md.")}
	testhelper.ProtoEqual(t, expectedResponse, response)
}

func TestFailedWikiWritePageDueToValidations(t *testing.T) {
	wikiRepo := &gitalypb.Repository{}

	cfg := testcfg.Build(t)
	client := setupWikiService(t, cfg, nil)

	commitDetails := &gitalypb.WikiCommitDetails{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad@gitlab.com"),
		Message:  []byte("Add installation instructions"),
		UserId:   int32(1),
		UserName: []byte("ahmad"),
	}

	testCases := []struct {
		desc    string
		request *gitalypb.WikiWritePageRequest
		code    codes.Code
	}{
		{
			desc: "empty name",
			request: &gitalypb.WikiWritePageRequest{
				Repository:    wikiRepo,
				Format:        "markdown",
				CommitDetails: commitDetails,
				Content:       []byte(""),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty format",
			request: &gitalypb.WikiWritePageRequest{
				Repository:    wikiRepo,
				Name:          []byte("Installing Gitaly"),
				CommitDetails: commitDetails,
				Content:       []byte(""),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details",
			request: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepo,
				Name:       []byte("Installing Gitaly"),
				Format:     "markdown",
				Content:    []byte(""),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' name",
			request: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepo,
				Name:       []byte("Installing Gitaly"),
				Format:     "markdown",
				CommitDetails: &gitalypb.WikiCommitDetails{
					Email:    []byte("a@b.com"),
					Message:  []byte("A message"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
				Content: []byte(""),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' email",
			request: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepo,
				Name:       []byte("Installing Gitaly"),
				Format:     "markdown",
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     []byte("A name"),
					Message:  []byte("A message"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
				Content: []byte(""),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty commit details' message",
			request: &gitalypb.WikiWritePageRequest{
				Repository: wikiRepo,
				Name:       []byte("Installing Gitaly"),
				Format:     "markdown",
				CommitDetails: &gitalypb.WikiCommitDetails{
					Name:     []byte("A name"),
					Email:    []byte("a@b.com"),
					UserId:   int32(1),
					UserName: []byte("username"),
				},
				Content: []byte(""),
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.WikiWritePage(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(testCase.request))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
