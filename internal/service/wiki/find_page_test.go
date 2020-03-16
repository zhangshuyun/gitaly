package wiki

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulWikiFindPageRequest(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	page1Name := "Home Pagé"
	page2Name := "Instálling/Step 133-b"
	page3Name := "Installing/Step 133-c"
	page4Name := "Encoding is fun"
	page5Name := "Empty file"
	page1Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Name})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page3Name})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page4Name, content: []byte("f\xFCr")})
	page5Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page5Name, forceContentEmpty: true})
	latestCommit := page5Commit

	testCases := []struct {
		desc            string
		request         *gitalypb.WikiFindPageRequest
		expectedPage    *gitalypb.WikiPage
		expectedContent []byte
	}{
		{
			desc: "title only",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: latestCommit,
					Format: "markdown",
				},
				Title:      []byte(page1Name),
				Format:     "markdown",
				UrlPath:    "Home-Pagé",
				Path:       []byte("Home-Pagé.md"),
				Name:       []byte(page1Name),
				Historical: false,
			},
			expectedContent: mockPageContent,
		},
		{
			desc: "title + revision that includes the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
				Revision:   []byte(page1Commit.Id),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: page1Commit,
					Format: "markdown",
				},
				Title:      []byte(page1Name),
				Format:     "markdown",
				UrlPath:    "Home-Pagé",
				Path:       []byte("Home-Pagé.md"),
				Name:       []byte(page1Name),
				Historical: true,
			},
			expectedContent: mockPageContent,
		},
		{
			desc: "title + revision that does not include the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page2Name),
				Revision:   []byte(page1Commit.Id),
			},
			expectedPage: nil,
		},
		{
			desc: "title + directory that includes the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte("Step 133-b"),
				Directory:  []byte("Instálling"),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: latestCommit,
					Format: "markdown",
				},
				Title:      []byte("Step 133 b"),
				Format:     "markdown",
				UrlPath:    "Instálling/Step-133-b",
				Path:       []byte("Instálling/Step-133-b.md"),
				Name:       []byte("Step 133 b"),
				Historical: false,
			},
			expectedContent: mockPageContent,
		},
		{
			desc: "title + directory that does not include the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte("Step 133-b"),
				Directory:  []byte("Installation"),
			},
			expectedPage: nil,
		},
		{
			desc: "title for invalidly-encoded page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte("Encoding is fun"),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: latestCommit,
					Format: "markdown",
				},
				Title:      []byte(page4Name),
				Format:     "markdown",
				UrlPath:    "Encoding-is-fun",
				Path:       []byte("Encoding-is-fun.md"),
				Name:       []byte(page4Name),
				Historical: false,
			},
			expectedContent: []byte("fr"),
		},
		{
			desc: "title for file with empty content",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte("Empty file"),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: latestCommit,
					Format: "markdown",
				},
				Title:      []byte(page5Name),
				Format:     "markdown",
				UrlPath:    "Empty-file",
				Path:       []byte("Empty-file.md"),
				Name:       []byte(page5Name),
				Historical: false,
			},
			expectedContent: nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WikiFindPage(ctx, testCase.request)
			require.NoError(t, err)

			expectedPage := testCase.expectedPage
			receivedPage := readFullWikiPageFromWikiFindPageClient(t, c)

			// require.Equal doesn't display a proper diff when either expected/actual has a field
			// with large data (RawData in our case), so we compare page attributes and content separately.
			receivedContent := receivedPage.GetRawData()
			if receivedPage != nil {
				receivedPage.RawData = nil
			}

			require.Equal(t, expectedPage, receivedPage, "mismatched page attributes")
			if expectedPage != nil {
				require.Equal(t, testCase.expectedContent, receivedContent, "mismatched page content")
			}
		})
	}
}

func TestSuccessfulWikiFindPageSameTitleDifferentPathRequest(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	page1Name := "page1"
	page1Content := []byte("content " + page1Name)

	page2Name := "page1"
	page2Path := "foo/" + page2Name
	page2Content := []byte("content " + page2Name)

	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name, content: page1Content})
	page2Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Path, content: page2Content})

	testCases := []struct {
		desc         string
		request      *gitalypb.WikiFindPageRequest
		expectedPage *gitalypb.WikiPage
		content      []byte
	}{
		{
			desc: "finding page in root directory by title only",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: page2Commit,
					Format: "markdown",
				},
				Title:      []byte(page1Name),
				Format:     "markdown",
				UrlPath:    "page1",
				Path:       []byte("page1.md"),
				Name:       []byte(page1Name),
				Historical: false,
			},
			content: page1Content,
		},
		{
			desc: "finding page in root directory by title + directory that includes the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
				Directory:  []byte(""),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: page2Commit,
					Format: "markdown",
				},
				Title:      []byte(page1Name),
				Format:     "markdown",
				UrlPath:    "page1",
				Path:       []byte("page1.md"),
				Name:       []byte(page1Name),
				Historical: false,
			},
			content: page1Content,
		},
		{
			desc: "finding page inside a directory by title + directory that includes the page",
			request: &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(page2Name),
				Directory:  []byte("foo"),
			},
			expectedPage: &gitalypb.WikiPage{
				Version: &gitalypb.WikiPageVersion{
					Commit: page2Commit,
					Format: "markdown",
				},
				Title:      []byte(page2Name),
				Format:     "markdown",
				UrlPath:    "foo/page1",
				Path:       []byte("foo/page1.md"),
				Name:       []byte(page2Name),
				Historical: false,
			},
			content: page2Content,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WikiFindPage(ctx, testCase.request)
			require.NoError(t, err)

			expectedPage := testCase.expectedPage
			receivedPage := readFullWikiPageFromWikiFindPageClient(t, c)

			// require.Equal doesn't display a proper diff when either expected/actual has a field
			// with large data (RawData in our case), so we compare page attributes and content separately.
			receivedContent := receivedPage.GetRawData()
			if receivedPage != nil {
				receivedPage.RawData = nil
			}

			require.Equal(t, expectedPage, receivedPage, "mismatched page attributes")
			if expectedPage != nil {
				require.Equal(t, testCase.content, receivedContent, "mismatched page content")
			}
		})
	}
}

func TestFailedWikiFindPageDueToValidation(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc  string
		title string
		code  codes.Code
	}{
		{
			desc:  "empty page path",
			title: "",
			code:  codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.WikiFindPageRequest{
				Repository: wikiRepo,
				Title:      []byte(testCase.title),
			}

			c, err := client.WikiFindPage(ctx, request)
			require.NoError(t, err)

			err = drainWikiFindPageResponse(c)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func drainWikiFindPageResponse(c gitalypb.WikiService_WikiFindPageClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func readFullWikiPageFromWikiFindPageClient(t *testing.T, c gitalypb.WikiService_WikiFindPageClient) (wikiPage *gitalypb.WikiPage) {
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if wikiPage == nil {
			wikiPage = resp.GetPage()
		} else {
			wikiPage.RawData = append(wikiPage.RawData, resp.GetPage().GetRawData()...)
		}
	}

	return wikiPage
}

func TestInvalidWikiFindPageRequestRevision(t *testing.T) {
	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.WikiFindPage(ctx, &gitalypb.WikiFindPageRequest{
		Repository: wikiRepo,
		Title:      []byte("non-empty title"),
		Revision:   []byte("--output=/meow"),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
