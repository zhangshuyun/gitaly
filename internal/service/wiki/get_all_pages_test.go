package wiki

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulWikiGetAllPagesRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	expectedPages := createTestWikiPages(t, client, wikiRepo)

	testcases := []struct {
		desc          string
		limit         uint32
		expectedCount int
	}{
		{
			desc:          "No limit",
			limit:         0,
			expectedCount: 3,
		},
		{
			desc:          "Limit of 1",
			limit:         1,
			expectedCount: 1,
		},
		{
			desc:          "Limit of 3",
			limit:         3,
			expectedCount: 3,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.WikiGetAllPagesRequest{Repository: wikiRepo, Limit: tc.limit}

			c, err := client.WikiGetAllPages(ctx, &rpcRequest)
			require.NoError(t, err)

			receivedPages := readWikiPagesFromWikiGetAllPagesClient(t, c)

			require.Len(t, receivedPages, tc.expectedCount)

			for i := 0; i < tc.expectedCount; i++ {
				requireWikiPagesEqual(t, expectedPages[i], receivedPages[i])
			}
		})
	}
}

func TestWikiGetAllPagesSorting(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	expectedPages := createTestWikiPages(t, client, wikiRepo)

	testcasesWithSorting := []struct {
		desc          string
		limit         uint32
		sort          gitalypb.WikiGetAllPagesRequest_SortBy
		directionDesc bool
		expectedCount int
	}{
		{
			desc:          "Sorting by title with no limit",
			limit:         0,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by title with limit of 1",
			limit:         1,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_TITLE,
			expectedCount: 1,
		},
		{
			desc:          "Sorting by title with limit of 3",
			limit:         3,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by title with limit of 3 and reversed direction",
			limit:         3,
			directionDesc: true,
			sort:          gitalypb.WikiGetAllPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with no limit",
			limit:         0,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with limit of 1",
			limit:         1,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_CREATED_AT,
			expectedCount: 1,
		},
		{
			desc:          "Sorting by created_at with limit of 3",
			limit:         3,
			directionDesc: false,
			sort:          gitalypb.WikiGetAllPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with limit of 3 and reversed direction",
			limit:         3,
			directionDesc: true,
			sort:          gitalypb.WikiGetAllPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
	}

	expectedSortedByCreatedAtPages := []*gitalypb.WikiPage{expectedPages[1], expectedPages[0], expectedPages[2]}

	for _, tc := range testcasesWithSorting {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.WikiGetAllPagesRequest{Repository: wikiRepo, Limit: tc.limit, DirectionDesc: tc.directionDesc, Sort: tc.sort}

			c, err := client.WikiGetAllPages(ctx, &rpcRequest)
			require.NoError(t, err)

			receivedPages := readWikiPagesFromWikiGetAllPagesClient(t, c)

			require.Len(t, receivedPages, tc.expectedCount)

			if tc.sort == gitalypb.WikiGetAllPagesRequest_CREATED_AT {
				expectedPages = expectedSortedByCreatedAtPages
			}

			for i := 0; i < tc.expectedCount; i++ {
				var index int
				if tc.directionDesc {
					index = tc.expectedCount - i - 1
				} else {
					index = i
				}

				requireWikiPagesEqual(t, expectedPages[index], receivedPages[i])
			}
		})
	}
}

func TestFailedWikiGetAllPagesDueToValidation(t *testing.T) {
	stop, serverSocketPath := runWikiServiceServer(t)
	defer stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc string
		req  *gitalypb.WikiGetAllPagesRequest
	}{
		{desc: "no repository", req: &gitalypb.WikiGetAllPagesRequest{}},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WikiGetAllPages(ctx, tc.req)
			require.NoError(t, err)

			err = drainWikiGetAllPagesResponse(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func createTestWikiPages(t *testing.T, client gitalypb.WikiServiceClient, wikiRepo *gitalypb.Repository) []*gitalypb.WikiPage {
	page1Name := "Page 1"
	page2Name := "Page 2"
	page3Name := "Page 3"
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Name, forceContentEmpty: true})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name})
	page3Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page3Name})
	expectedPage1 := &gitalypb.WikiPage{
		Version:    &gitalypb.WikiPageVersion{Commit: page3Commit, Format: "markdown"},
		Title:      []byte(page1Name),
		Format:     "markdown",
		UrlPath:    "Page-1",
		Path:       []byte("Page-1.md"),
		Name:       []byte(page1Name),
		RawData:    mockPageContent,
		Historical: false,
	}
	expectedPage2 := &gitalypb.WikiPage{
		Version:    &gitalypb.WikiPageVersion{Commit: page3Commit, Format: "markdown"},
		Title:      []byte(page2Name),
		Format:     "markdown",
		UrlPath:    "Page-2",
		Path:       []byte("Page-2.md"),
		Name:       []byte(page2Name),
		RawData:    nil,
		Historical: false,
	}
	expectedPage3 := &gitalypb.WikiPage{
		Version:    &gitalypb.WikiPageVersion{Commit: page3Commit, Format: "markdown"},
		Title:      []byte(page3Name),
		Format:     "markdown",
		UrlPath:    "Page-3",
		Path:       []byte("Page-3.md"),
		Name:       []byte(page3Name),
		RawData:    mockPageContent,
		Historical: false,
	}

	return []*gitalypb.WikiPage{expectedPage1, expectedPage2, expectedPage3}
}

func readWikiPagesFromWikiGetAllPagesClient(t *testing.T, c gitalypb.WikiService_WikiGetAllPagesClient) []*gitalypb.WikiPage {
	var wikiPage *gitalypb.WikiPage
	var wikiPages []*gitalypb.WikiPage

	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if resp.EndOfPage {
			wikiPages = append(wikiPages, wikiPage)
			wikiPage = nil
		} else if wikiPage == nil {
			wikiPage = resp.GetPage()
		} else {
			wikiPage.RawData = append(wikiPage.RawData, resp.GetPage().GetRawData()...)
		}
	}

	return wikiPages
}

func drainWikiGetAllPagesResponse(c gitalypb.WikiService_WikiGetAllPagesClient) error {
	for {
		_, err := c.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
}
