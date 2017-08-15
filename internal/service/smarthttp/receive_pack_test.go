package smarthttp

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
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
	server := runSmartHTTPServer(t)
	defer server.Stop()

	storagePath := testhelper.GitlabTestStoragePath()
	remoteRepoRelativePath := "gitlab-test-remote"
	localRepoRelativePath := "gitlab-test-local"
	testRepoPath := path.Join(storagePath, testRepo.RelativePath)
	remoteRepoPath := path.Join(storagePath, remoteRepoRelativePath)
	localRepoPath := path.Join(storagePath, localRepoRelativePath)
	// Make a non-bare clone of the test repo to act as a local one
	testhelper.MustRunCommand(t, nil, "git", "clone", testRepoPath, localRepoPath)
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, remoteRepoPath)
	defer os.RemoveAll(remoteRepoPath)
	defer os.RemoveAll(localRepoPath)

	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	clientCapabilities := "report-status side-band-64k agent=git/2.12.0"

	// The latest commit ID on the remote repo
	oldHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", commitMsg)

	// The commit ID we want to push to the remote repo
	newHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	// ReceivePack request is a packet line followed by a packet flush, then the pack file of the objects we want to push.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_uploading_data
	// We form the packet line the same way git executable does: https://github.com/git/git/blob/d1a13d3fcb252631361a961cb5e2bf10ed467cba/send-pack.c#L524-L527
	pkt := fmt.Sprintf("%s %s refs/heads/master\x00 %s", oldHead, newHead, clientCapabilities)
	// We need to get a pack file containing the objects we want to push, so we use git pack-objects
	// which expects a list of revisions passed through standard input. The list format means
	// pack the objects needed if I have oldHead but not newHead (think of it from the perspective of the remote repo).
	// For more info, check the man pages of both `git-pack-objects` and `git-rev-list --objects`.
	stdin := bytes.NewBufferString(fmt.Sprintf("^%s\n%s\n", oldHead, newHead))
	// The options passed are the same ones used when doing an actual push.
	pack := testhelper.MustRunCommand(t, stdin, "git", "-C", localRepoPath, "pack-objects", "--stdout", "--revs", "--thin", "--delta-base-offset", "-q")

	// We chop the request into multiple small pieces to exercise the server code that handles
	// the stream sent by the client, so we use a buffer to read chunks of data in a nice way.
	requestBuffer := &bytes.Buffer{}
	fmt.Fprintf(requestBuffer, "%04x%s%s", len(pkt)+4, pkt, pktFlushStr)
	requestBuffer.Write(pack)

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: remoteRepoRelativePath}
	rpcRequest := &pb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.PostReceivePack(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(rpcRequest))

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.PostReceivePackRequest{Data: p})
	})
	_, err = io.Copy(sw, requestBuffer)
	require.NoError(t, err)

	stream.CloseSend()

	// Verify everything is going as planned
	responseBuffer := bytes.Buffer{}
	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	_, err = io.Copy(&responseBuffer, rr)
	require.NoError(t, err)

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	require.Equal(t, expectedResponse, responseBuffer.String(), "Expected response to be %q, got %q", expectedResponse, responseBuffer.String())

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "show", string(newHead))
}

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
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
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
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
