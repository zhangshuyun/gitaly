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

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulReceivePackRequest(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	remoteRepoPath := path.Join(testRepoRoot, "gitlab-test-remote")
	localRepoPath := path.Join(testRepoRoot, "gitlab-test-local")
	// Make a non-bare clone of the test repo to act as a local one
	testhelper.MustRunCommand(t, nil, "git", "clone", testhelper.GitlabTestRepoPath(), localRepoPath)
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testhelper.GitlabTestRepoPath(), remoteRepoPath)
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

	client := newSmartHTTPClient(t)
	repo := &pb.Repository{Path: remoteRepoPath}
	rpcRequest := &pb.PostReceivePackRequest{Repository: repo, GlId: "user-123", GlRepository: "project-123"}
	stream, err := client.PostReceivePack(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(rpcRequest); err != nil {
		t.Fatal(err)
	}

	data := make([]byte, 16)
	for {
		n, err := requestBuffer.Read(data)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		rpcRequest = &pb.PostReceivePackRequest{Data: data[:n]}
		if err := stream.Send(rpcRequest); err != nil {
			t.Fatal(err)
		}
	}
	stream.CloseSend()

	// Verify everything is going as planned
	responseBuffer := bytes.Buffer{}
	for {
		rpcResponse, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				t.Fatal(err)
			}
		}

		responseBuffer.Write(rpcResponse.GetData())
	}

	expectedResponse := "0030\x01000eunpack ok\n0019ok refs/heads/master\n00000000"
	if responseBuffer.String() != expectedResponse {
		t.Errorf("Expected response to be %q, got %q", expectedResponse, responseBuffer.String())
	}

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "show", string(newHead))
}

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client := newSmartHTTPClient(t)

	rpcRequests := []pb.PostReceivePackRequest{
		{Repository: &pb.Repository{Path: ""}, GlId: "user-123"},                                    // Repository.Path is empty
		{Repository: nil, GlId: "user-123"},                                                         // Repository is nil
		{Repository: &pb.Repository{Path: "/path/to/repo"}, GlId: ""},                               // Empty GlId
		{Repository: &pb.Repository{Path: "/path/to/repo"}, GlId: "user-123", Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Logf("test case: %v", rpcRequest)
		stream, err := client.PostReceivePack(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if err := stream.Send(&rpcRequest); err != nil {
			t.Fatal(err)
		}
		stream.CloseSend()

		err = drainPostReceivePackResponse(stream)
		testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
	}
}

func drainPostReceivePackResponse(stream pb.SmartHTTP_PostReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
