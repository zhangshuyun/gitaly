package smarthttp

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulReceivePackRequest(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	push := newTestPush(t, nil)
	firstRequest := &pb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123"}
	response := doPush(t, stream, firstRequest, push.body)

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	require.Equal(t, expectedResponse, string(response), "Expected response to be %q, got %q", expectedResponse, response)

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show", push.newHead)
}

func doPush(t *testing.T, stream pb.SmartHTTPService_PostReceivePackClient, firstRequest *pb.PostReceivePackRequest, body io.Reader) []byte {
	require.NoError(t, stream.Send(firstRequest))

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.PostReceivePackRequest{Data: p})
	})
	_, err := io.Copy(sw, body)
	require.NoError(t, err)

	require.NoError(t, stream.CloseSend())

	responseBuffer := bytes.Buffer{}
	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	_, err = io.Copy(&responseBuffer, rr)
	require.NoError(t, err)

	return responseBuffer.Bytes()
}

type pushData struct {
	newHead string
	body    io.Reader
}

func newTestPush(t *testing.T, fileContents []byte) *pushData {
	_, repoPath, localCleanup := testhelper.NewTestRepoWithWorktree(t)
	defer localCleanup()

	oldHead, newHead := createCommit(t, repoPath, fileContents)

	// ReceivePack request is a packet line followed by a packet flush, then the pack file of the objects we want to push.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_uploading_data
	// We form the packet line the same way git executable does: https://github.com/git/git/blob/d1a13d3fcb252631361a961cb5e2bf10ed467cba/send-pack.c#L524-L527
	clientCapabilities := "report-status side-band-64k agent=git/2.12.0"
	pkt := fmt.Sprintf("%s %s refs/heads/master\x00 %s", oldHead, newHead, clientCapabilities)

	// We need to get a pack file containing the objects we want to push, so we use git pack-objects
	// which expects a list of revisions passed through standard input. The list format means
	// pack the objects needed if I have oldHead but not newHead (think of it from the perspective of the remote repo).
	// For more info, check the man pages of both `git-pack-objects` and `git-rev-list --objects`.
	stdin := strings.NewReader(fmt.Sprintf("^%s\n%s\n", oldHead, newHead))

	// The options passed are the same ones used when doing an actual push.
	pack := testhelper.MustRunCommand(t, stdin, "git", "-C", repoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")

	// We chop the request into multiple small pieces to exercise the server code that handles
	// the stream sent by the client, so we use a buffer to read chunks of data in a nice way.
	requestBuffer := &bytes.Buffer{}
	fmt.Fprintf(requestBuffer, "%04x%s%s", len(pkt)+4, pkt, pktFlushStr)
	requestBuffer.Write(pack)

	return &pushData{newHead: newHead, body: requestBuffer}
}

// createCommit creates a commit on HEAD with a file containing the
// specified contents.
func createCommit(t *testing.T, repoPath string, fileContents []byte) (oldHead string, newHead string) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// The latest commit ID on the remote repo
	oldHead = strings.TrimSpace(string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "master")))

	changedFile := "README.md"
	require.NoError(t, ioutil.WriteFile(path.Join(repoPath, changedFile), fileContents, 0644))

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "add", changedFile)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", commitMsg)

	// The commit ID we want to push to the remote repo
	newHead = strings.TrimSpace(string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "master")))

	return oldHead, newHead
}

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	rpcRequests := []pb.PostReceivePackRequest{
		{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}, GlId: "user-123"},                                  // Repository doesn't exist
		{Repository: nil, GlId: "user-123"},                                                                                        // Repository is nil
		{Repository: &pb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, GlId: ""},                               // Empty GlId
		{Repository: &pb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, GlId: "user-123", Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.PostReceivePack(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&rpcRequest))
			stream.CloseSend()

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func drainPostReceivePackResponse(stream pb.SmartHTTP_PostReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
