package wiki

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulWikiFindFileRequest(t *testing.T) {
	_, wikiRepoPath, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	storagePath := testhelper.GitlabTestStoragePath()
	sandboxWikiPath := path.Join(storagePath, "find-file-sandbox")

	testhelper.MustRunCommand(t, nil, "git", "clone", wikiRepoPath, sandboxWikiPath)
	defer os.RemoveAll(sandboxWikiPath)

	sandboxWiki := &pb.Repository{
		StorageName:  "default",
		RelativePath: "find-file-sandbox/.git",
	}

	content, err := ioutil.ReadFile("testdata/clouds.png")
	require.NoError(t, err)

	err = ioutil.WriteFile(path.Join(sandboxWikiPath, "cloúds.png"), content, 0777)
	require.NoError(t, err)

	// Sandbox wiki is empty, so we create a commit to be used later
	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxWikiPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "Adding an empty commit")
	oldHeadID := testhelper.MustRunCommand(t, nil, "git", "-C", sandboxWikiPath, "show", "--format=format:%H", "--no-patch", "HEAD")

	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxWikiPath, "add", ".")
	testhelper.MustRunCommand(t, nil, "git", "-C", sandboxWikiPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", "Adding an image")

	newHeadID := testhelper.MustRunCommand(t, nil, "git", "-C", sandboxWikiPath, "show", "--format=format:%H", "--no-patch", "HEAD")

	response := &pb.WikiFindFileResponse{
		Name:     []byte("cloúds.png"),
		MimeType: "image/png",
		Path:     []byte("cloúds.png"),
	}

	testCases := []struct {
		desc     string
		request  *pb.WikiFindFileRequest
		response *pb.WikiFindFileResponse
	}{
		{
			desc: "name only",
			request: &pb.WikiFindFileRequest{
				Repository: sandboxWiki,
				Name:       []byte("cloúds.png"),
			},
			response: response,
		},
		{
			desc: "name + revision that includes the file",
			request: &pb.WikiFindFileRequest{
				Repository: sandboxWiki,
				Name:       []byte("cloúds.png"),
				Revision:   newHeadID,
			},
			response: response,
		},
		{
			desc: "name + revision that does not include the file",
			request: &pb.WikiFindFileRequest{
				Repository: sandboxWiki,
				Name:       []byte("cloúds.png"),
				Revision:   oldHeadID,
			},
			response: &pb.WikiFindFileResponse{},
		},
		{
			desc: "non-existent name",
			request: &pb.WikiFindFileRequest{
				Repository: sandboxWiki,
				Name:       []byte("moar-clouds.png"),
			},
			response: &pb.WikiFindFileResponse{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WikiFindFile(ctx, testCase.request)
			require.NoError(t, err)

			expectedResponse := testCase.response
			receivedResponse := readFullResponseFromWikiFindFileClient(t, c)

			// require.Equal doesn't display a proper diff when either expected/actual has a field
			// with large data (RawData in our case), so we compare file attributes and content separately.
			receivedContent := receivedResponse.GetRawData()
			if receivedResponse != nil {
				receivedResponse.RawData = nil
			}

			require.Equal(t, expectedResponse, receivedResponse, "mismatched response")
			if len(expectedResponse.Name) > 0 {
				require.Equal(t, content, receivedContent, "mismatched content")
			}
		})
	}
}

func TestFailedWikiFindFileDueToValidation(t *testing.T) {
	wikiRepo, _, cleanupFunc := setupWikiRepo(t)
	defer cleanupFunc()

	server, serverSocketPath := runWikiServiceServer(t)
	defer server.Stop()

	client, conn := newWikiClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc     string
		name     string
		revision string
		code     codes.Code
	}{
		{
			desc:     "empty file path",
			name:     "",
			revision: "master",
			code:     codes.InvalidArgument,
		},
		{
			desc:     "invalid revision",
			name:     "image.jpg",
			revision: "deadfacedeadfacedeadfacedeadfacedeadface",
			code:     codes.Unknown,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.WikiFindFileRequest{
				Repository: wikiRepo,
				Name:       []byte(testCase.name),
				Revision:   []byte(testCase.revision),
			}

			c, err := client.WikiFindFile(ctx, request)
			require.NoError(t, err)

			err = drainWikiFindFileResponse(c)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func drainWikiFindFileResponse(c pb.WikiService_WikiFindFileClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func readFullResponseFromWikiFindFileClient(t *testing.T, c pb.WikiService_WikiFindFileClient) (fullResponse *pb.WikiFindFileResponse) {
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if fullResponse == nil {
			fullResponse = resp
		} else {
			fullResponse.RawData = append(fullResponse.RawData, resp.GetRawData()...)
		}
	}

	return fullResponse
}
