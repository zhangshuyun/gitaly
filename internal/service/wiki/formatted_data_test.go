package wiki

import (
	"bytes"
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulWikiGetFormattedDataRequest(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	format := "rdoc"
	content := bytes.Repeat([]byte("*bold*\n\n"), 10000)
	expectedContent := bytes.Repeat([]byte("\n<p><strong>bold</strong></p>\n"), 10000)
	page1Name := "Home Pagé"
	page2Name := "Instálling/Step 133-b"
	page3Name := "Installing/Step 133-c"
	page1Commit := createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page1Name, format: format, content: content})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page2Name, format: format, content: content})
	createTestWikiPage(t, client, wikiRepo, createWikiPageOpts{title: page3Name, format: format, content: content})

	testCases := []struct {
		desc    string
		request *pb.WikiGetFormattedDataRequest
	}{
		{
			desc: "title only",
			request: &pb.WikiGetFormattedDataRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
			},
		},
		{
			desc: "title + revision that includes the page",
			request: &pb.WikiGetFormattedDataRequest{
				Repository: wikiRepo,
				Title:      []byte(page1Name),
				Revision:   []byte(page1Commit.Id),
			},
		},
		{
			desc: "title + directory that includes the page",
			request: &pb.WikiGetFormattedDataRequest{
				Repository: wikiRepo,
				Title:      []byte("Step 133-b"),
				Directory:  []byte("Instálling"),
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WikiGetFormattedData(ctx, testCase.request)
			require.NoError(t, err)

			receivedData := readFullDataFromWikiGetFormattedDataClient(t, c)
			require.Equal(t, expectedContent, receivedData)
		})
	}
}

func TestFailedWikiGetFormattedDataDueToValidation(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

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
		{
			desc:  "non-existent page",
			title: "i-do-not-exist",
			code:  codes.NotFound,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.WikiGetFormattedDataRequest{
				Repository: wikiRepo,
				Title:      []byte(testCase.title),
			}

			c, err := client.WikiGetFormattedData(ctx, request)
			require.NoError(t, err)

			err = drainWikiGetFormattedDataResponse(c)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func drainWikiGetFormattedDataResponse(c pb.WikiService_WikiGetFormattedDataClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func readFullDataFromWikiGetFormattedDataClient(t *testing.T, c pb.WikiService_WikiGetFormattedDataClient) (data []byte) {
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		data = append(data, resp.GetData()...)
	}

	return
}
