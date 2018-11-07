package wiki

import (
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestSuccessfulWikiListPagesRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	page1Name := "Page 1"
	page2Name := "Page 2"
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Name})
	// expectedPage1 := &gitalypb.WikiPage{
	// 	Version:    &gitalypb.WikiPageVersion{Commit: page2Commit, Format: "markdown"},
	// 	Title:      []byte(page1Name),
	// 	Format:     "markdown",
	// 	UrlPath:    "Page-1",
	// 	Path:       []byte("Page-1.md"),
	// 	Name:       []byte(page1Name),
	// 	RawData:    nil,
	// 	Historical: false,
	// }
	// expectedPage2 := &gitalypb.WikiPage{
	// 	Version:    &gitalypb.WikiPageVersion{Commit: page2Commit, Format: "markdown"},
	// 	Title:      []byte(page2Name),
	// 	Format:     "markdown",
	// 	UrlPath:    "Page-2",
	// 	Path:       []byte("Page-2.md"),
	// 	Name:       []byte(page2Name),
	// 	RawData:    nil,
	// 	Historical: false,
	// }

	testcases := []struct {
		desc          string
		limit         uint32
		expectedCount int
	}{
		{
			desc:          "No limit",
			limit:         0,
			expectedCount: 2,
		},
		// {
		// 	desc:          "Limit of 1",
		// 	limit:         1,
		// 	expectedCount: 1,
		// },
		// {
		// 	desc:          "Limit of 2",
		// 	limit:         2,
		// 	expectedCount: 2,
		// },
	}

	// expectedPages := []*gitalypb.WikiPage{expectedPage1, expectedPage2}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.WikiListPagesRequest{Repository: wikiRepo, Limit: tc.limit}

			stream, err := client.WikiListPages(ctx, &rpcRequest)
			require.NoError(t, err)
			require.NoError(t, stream.CloseSend())

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					log.Print("END")
					break
				} else if err != nil {
					t.Fatal(err)
				}

				log.Print(resp.GetPage())
			}

			// response, err := stream.Recv()
			// require.NoError(t, err)
			//
			// log.Print(response.GetPages())
			// response, _ = stream.Recv()
			// log.Print(response.GetPages())

			// require.Len(t, response.GetPages(), tc.expectedCount)
			require.Equal(t, 1, 2)
			// for i, page := range response.GetPages() {
			// 	requireWikiPagesEqual(t, expectedPages[i], page)
			// }
		})
	}
}

// func TestFailedWikiListPagesDueToValidation(t *testing.T) {
// 	server, serverSocketPath := runWikiServiceServer(t)
// 	defer server.Stop()
//
// 	client, conn := newWikiClient(t, serverSocketPath)
// 	defer conn.Close()
//
// 	rpcRequests := []gitalypb.WikiListPagesRequest{
// 		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}}, // Repository doesn't exist
// 		{Repository: nil}, // Repository is nil
// 	}
//
// 	for _, rpcRequest := range rpcRequests {
// 		ctx, cancel := testhelper.Context()
// 		defer cancel()
//
// 		c, err := client.WikiListPages(ctx, &rpcRequest)
// 		require.NoError(t, err)
//
// 		err = drainWikiListPagesResponse(c)
// 		testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
// 	}
// }
