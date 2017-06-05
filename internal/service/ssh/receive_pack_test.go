package ssh

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	client := newSSHClient(t)

	rpcRequests := []pb.SSHReceivePackRequest{
		{Repository: &pb.Repository{Path: ""}, GlId: "user-123"},                                     // Repository.Path is empty
		{Repository: nil, GlId: "user-123"},                                                          // Repository is nil
		{Repository: &pb.Repository{Path: "/path/to/repo"}, GlId: ""},                                // Empty GlId
		{Repository: &pb.Repository{Path: "/path/to/repo"}, GlId: "user-123", Stdin: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Logf("test case: %v", rpcRequest)
		stream, err := client.SSHReceivePack(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if err = stream.Send(&rpcRequest); err != nil {
			t.Fatal(err)
		}
		stream.CloseSend()

		err = drainPostReceivePackResponse(stream)
		testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
	}
}

func TestSuccessReceivePack(t *testing.T) {
	remoteRepoPath := path.Join(testRepoRoot, "gitlab-test-remote")
	localRepoPath := path.Join(testRepoRoot, "gitlab-test-local")
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, remoteRepoPath)
	// Make a non-bare clone of the test repo to act as a local one
	testhelper.MustRunCommand(t, nil, "git", "clone", remoteRepoPath, localRepoPath)
	// We need git thinking we're pushing over SSH...
	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "remote", "set-url", "origin", "git@localhost:test/test.git")
	defer os.RemoveAll(remoteRepoPath)
	defer os.RemoveAll(localRepoPath)

	makeCommit(t, localRepoPath)

	cmd := exec.Command("git", "-C", localRepoPath, "push", "origin", "master")
	cmd.Env = []string{
		// Running inside the context of the Repo... hence the ugly paths...
		fmt.Sprintf("GITALY_SOCKET=../../../%s", serverSocketPath),
		fmt.Sprintf("GL_REPOSITORY=%s", remoteRepoPath),
		"GL_ID=1",
		"GIT_SSH_COMMAND='../../../../../../gitaly-receive-pack'",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Error pushing: %v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		t.Fatalf("Failed to run `git clone`: %q", out)
	}

	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))
	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rev-parse", "master"))

	if bytes.Compare(localHead, remoteHead) != 0 {
		t.Errorf("local and remote head not equal. push failed: %q != %q", localHead, remoteHead)
	}
}

// makeCommit creates a new commit and returns oldHead, newHead, success
func makeCommit(t *testing.T, localRepoPath string) ([]byte, []byte, bool) {
	commitMsg := fmt.Sprintf("Testing ReceivePack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// The latest commit ID on the remote repo
	oldHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", commitMsg)
	if t.Failed() {
		return nil, nil, false
	}

	// The commit ID we want to push to the remote repo
	newHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))

	return oldHead, newHead, t.Failed()

}

func drainPostReceivePackResponse(stream pb.SSH_SSHReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
