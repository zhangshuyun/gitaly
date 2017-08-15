package commit

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCommitStatsUnimplemented(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.CommitStats(ctx, &pb.CommitStatsRequest{Repository: testRepo, Revision: []byte("master")})
	testhelper.AssertGrpcError(t, err, codes.Unimplemented, "not implemented")
}
