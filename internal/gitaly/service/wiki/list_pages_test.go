package wiki

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func testSuccessfulWikiListPagesRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	client := setupWikiService(t, cfg, rubySrv)

	wikiRepo, wikiRepoPath, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	expectedPages := createTestWikiPages(t, cfg, client, wikiRepo, wikiRepoPath)

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
			rpcRequest := gitalypb.WikiListPagesRequest{Repository: wikiRepo, Limit: tc.limit}

			c, err := client.WikiListPages(ctx, &rpcRequest)
			require.NoError(t, err)

			receivedPages := readWikiPagesFromWikiListPagesClient(t, c)

			require.Len(t, receivedPages, tc.expectedCount)

			for i := 0; i < tc.expectedCount; i++ {
				receivedPage := receivedPages[i]
				require.Equal(t, expectedPages[i].GetTitle(), receivedPage.GetTitle())
				require.Len(t, receivedPage.GetRawData(), 0, "page data should not be returned")
			}
		})
	}
}

func testWikiListPagesSorting(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	client := setupWikiService(t, cfg, rubySrv)

	wikiRepo, wikiRepoPath, cleanupFunc := setupWikiRepo(t, cfg)
	defer cleanupFunc()

	expectedPages := createTestWikiPages(t, cfg, client, wikiRepo, wikiRepoPath)

	testcasesWithSorting := []struct {
		desc          string
		limit         uint32
		sort          gitalypb.WikiListPagesRequest_SortBy
		directionDesc bool
		expectedCount int
	}{
		{
			desc:          "Sorting by title with no limit",
			limit:         0,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by title with limit of 1",
			limit:         1,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_TITLE,
			expectedCount: 1,
		},
		{
			desc:          "Sorting by title with limit of 3",
			limit:         3,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by title with limit of 3 and reversed direction",
			limit:         3,
			directionDesc: true,
			sort:          gitalypb.WikiListPagesRequest_TITLE,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with no limit",
			limit:         0,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with limit of 1",
			limit:         1,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_CREATED_AT,
			expectedCount: 1,
		},
		{
			desc:          "Sorting by created_at with limit of 3",
			limit:         3,
			directionDesc: false,
			sort:          gitalypb.WikiListPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
		{
			desc:          "Sorting by created_at with limit of 3 and reversed direction",
			limit:         3,
			directionDesc: true,
			sort:          gitalypb.WikiListPagesRequest_CREATED_AT,
			expectedCount: 3,
		},
	}

	expectedSortedByCreatedAtPages := []*gitalypb.WikiPage{expectedPages[1], expectedPages[0], expectedPages[2]}

	for _, tc := range testcasesWithSorting {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.WikiListPagesRequest{Repository: wikiRepo, Limit: tc.limit, DirectionDesc: tc.directionDesc, Sort: tc.sort}

			c, err := client.WikiListPages(ctx, &rpcRequest)
			require.NoError(t, err)

			receivedPages := readWikiPagesFromWikiListPagesClient(t, c)

			require.Len(t, receivedPages, tc.expectedCount)

			if tc.sort == gitalypb.WikiListPagesRequest_CREATED_AT {
				expectedPages = expectedSortedByCreatedAtPages
			}

			for i := 0; i < tc.expectedCount; i++ {
				var index int
				if tc.directionDesc {
					index = tc.expectedCount - i - 1
				} else {
					index = i
				}

				receivedPage := receivedPages[i]
				require.Equal(t, expectedPages[index].GetTitle(), receivedPage.GetTitle())
				require.Len(t, receivedPage.GetRawData(), 0, "page data should not be returned")
			}
		})
	}
}

func readWikiPagesFromWikiListPagesClient(t *testing.T, c gitalypb.WikiService_WikiListPagesClient) []*gitalypb.WikiPage {
	var wikiPages []*gitalypb.WikiPage

	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}

		wikiPages = append(wikiPages, resp.GetPage())
	}

	return wikiPages
}
