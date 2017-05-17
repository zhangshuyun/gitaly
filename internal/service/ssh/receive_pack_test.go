package ssh

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestFailedReceivePackRequestDueToValidationError(t *testing.T) {
	server := runSSHServer(t)
	defer server.Stop()

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

func drainPostReceivePackResponse(stream pb.SSH_SSHReceivePackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}
