package ssh

import (
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

func drainPostUploadPackResponse(stream pb.SSH_SSHUploadPackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
