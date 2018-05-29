package ssh

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *pb.SSHReceivePackRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &pb.SSHReceivePackRequest{Repository: &pb.Repository{StorageName: "default", RelativePath: ""}, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &pb.SSHReceivePackRequest{Repository: nil, GlId: "user-123"},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Empty GlId",
			Req:  &pb.SSHReceivePackRequest{Repository: &pb.Repository{StorageName: "default", RelativePath: testRepo.GetRelativePath()}, GlId: ""},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &pb.SSHReceivePackRequest{Repository: &pb.Repository{StorageName: "default", RelativePath: testRepo.GetRelativePath()}, GlId: "user-123", Stdin: []byte("Fail")},
			Code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SSHReceivePack(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			stream.CloseSend()

			err = drainPostReceivePackResponse(stream)
			testhelper.RequireGrpcError(t, err, test.Code)
		})
	}
}

func TestReceivePackPushSuccess(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	lHead, rHead, err := testCloneAndPush(t, serverSocketPath, testRepo.GetStorageName(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Compare(lHead, rHead) != 0 {
		t.Errorf("local and remote head not equal. push failed: %q != %q", lHead, rHead)
	}
}

func TestReceivePackPushFailure(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	_, _, err := testCloneAndPush(t, serverSocketPath, "foobar", "1")
	if err == nil {
		t.Errorf("local and remote head equal. push did not fail")
	}
	_, _, err = testCloneAndPush(t, serverSocketPath, testRepo.GetStorageName(), "")
	if err == nil {
		t.Errorf("local and remote head equal. push did not fail")
	}
}

func testCloneAndPush(t *testing.T, serverSocketPath string, storageName, glID string) (string, string, error) {
	storagePath := testhelper.GitlabTestStoragePath()
	tempRepo := "gitlab-test-ssh-receive-pack.git"
	testRepoPath := path.Join(storagePath, testRepo.GetRelativePath())
	remoteRepoPath := path.Join(storagePath, tempRepo)
	localRepoPath := path.Join(storagePath, "gitlab-test-ssh-receive-pack-local")
	// Make a bare clone of the test repo to act as a remote one and to leave the original repo intact for other tests
	if err := os.RemoveAll(remoteRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, remoteRepoPath)
	// Make a non-bare clone of the test repo to act as a local one
	if err := os.RemoveAll(localRepoPath); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	testhelper.MustRunCommand(t, nil, "git", "clone", remoteRepoPath, localRepoPath)
	// We need git thinking we're pushing over SSH...
	defer os.RemoveAll(remoteRepoPath)
	defer os.RemoveAll(localRepoPath)

	makeCommit(t, localRepoPath)

	pbTempRepo := &pb.Repository{StorageName: storageName, RelativePath: tempRepo}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&pb.SSHReceivePackRequest{
		Repository:   pbTempRepo,
		GlRepository: pbTempRepo.GetRelativePath(),
		GlId:         glID,
	})
	require.NoError(t, err)

	cmd := exec.Command("git", "-C", localRepoPath, "push", "-v", "git@localhost:test/test.git", "master")
	cmd.Env = []string{
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_ADDRESS=unix:%s", serverSocketPath),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s receive-pack`, gitalySSHPath),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("Error pushing: %v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		return "", "", fmt.Errorf("Failed to run `git push`: %q", out)
	}

	localHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "rev-parse", "master"))
	remoteHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rev-parse", "master"))

	return string(localHead), string(remoteHead), nil
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
