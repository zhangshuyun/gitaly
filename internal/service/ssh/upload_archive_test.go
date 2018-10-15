package ssh

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedUploadArchiveRequestDueToValidationError(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	client, conn := newSSHClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		Desc string
		Req  *gitalypb.SSHUploadArchiveRequest
		Code codes.Code
	}{
		{
			Desc: "Repository.RelativePath is empty",
			Req:  &gitalypb.SSHUploadArchiveRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: ""}},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Repository is nil",
			Req:  &gitalypb.SSHUploadArchiveRequest{Repository: nil},
			Code: codes.InvalidArgument,
		},
		{
			Desc: "Data exists on first request",
			Req:  &gitalypb.SSHUploadArchiveRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, Stdin: []byte("Fail")},
			Code: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.Desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.SSHUploadArchive(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if err = stream.Send(test.Req); err != nil {
				t.Fatal(err)
			}
			stream.CloseSend()

			err = drainUploadArchiveResponse(stream)
			testhelper.RequireGrpcError(t, err, test.Code)
		})
	}
}

func TestUploadArchiveSuccess(t *testing.T) {
	server, serverSocketPath := runSSHServer(t)
	defer server.Stop()

	cmd := exec.Command("git", "archive", "master", "--remote=git@localhost:test/test.git")

	err := testArchive(t, serverSocketPath, testRepo, cmd)
	require.NoError(t, err)
}

func testArchive(t *testing.T, serverSocketPath string, testRepo *gitalypb.Repository, cmd *exec.Cmd) error {
	req := &gitalypb.SSHUploadArchiveRequest{Repository: testRepo}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(req)

	require.NoError(t, err)

	cmd.Env = []string{
		fmt.Sprintf("GITALY_ADDRESS=unix:%s", serverSocketPath),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("PATH=%s", ".:"+os.Getenv("PATH")),
		fmt.Sprintf(`GIT_SSH_COMMAND=%s upload-archive`, gitalySSHPath),
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %q", err, out)
	}
	if !cmd.ProcessState.Success() {
		return fmt.Errorf("Failed to run `git archive`: %q", out)
	}

	return nil
}

func drainUploadArchiveResponse(stream gitalypb.SSHService_SSHUploadArchiveClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
