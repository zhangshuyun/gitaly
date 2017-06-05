package ssh

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	client := newSSHClient(t)

	rpcRequests := []pb.SSHUploadPackRequest{
		{Repository: &pb.Repository{Path: ""}}, // Repository.Path is empty
		{Repository: nil},                      // Repository is nil
		{Repository: &pb.Repository{Path: "/path/to/repo"}, Stdin: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Logf("test case: %v", rpcRequest)
		stream, err := client.SSHUploadPack(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if err = stream.Send(&rpcRequest); err != nil {
			t.Fatal(err)
		}
		stream.CloseSend()

		err = drainPostUploadPackResponse(stream)
		testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
	}
}

func TestSuccessUploadPack(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

	remoteRepoPath := path.Join(testRepoRoot, "gitlab-test-remote")
	localRepoPath := path.Join(testRepoRoot, "gitlab-test-local")
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, remoteRepoPath)
	defer os.RemoveAll(remoteRepoPath)
	defer os.RemoveAll(localRepoPath)

	cmd := exec.Command("git", "clone", fmt.Sprintf("git@localhost:%s", remoteRepoPath), localRepoPath)
	cmd.Env = []string{
		fmt.Sprintf("GITALY_SOCKET=%s", serverSocketPath),
		fmt.Sprintf("GL_REPOSITORY=%s", remoteRepoPath),
		fmt.Sprintf("GOPATH=%s", os.Getenv("GOPATH")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		"GIT_SSH_COMMAND=go run ./cmd/gitaly-upload-pack/main.go",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Error cloning: %v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		t.Fatalf("Failed to run `git clone`: %q", out)
	}
	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "status")
}

func drainPostUploadPackResponse(stream pb.SSH_SSHUploadPackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
