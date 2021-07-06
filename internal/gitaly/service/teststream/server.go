package teststream

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	gitalypb.UnimplementedTestStreamServiceServer
	locator storage.Locator
}

func (s *server) TestStream(ctx context.Context, request *gitalypb.TestStreamRequest) (*emptypb.Empty, error) {
	if _, err := s.locator.GetRepoPath(request.Repository); err != nil {
		return nil, err
	}

	c, err := streamrpc.AcceptConnection(ctx)
	if err != nil {
		return nil, err
	}

	_, err = io.CopyN(c, c, request.Size)
	return nil, err
}

// NewServer creates a new instance of a grpc ServerServiceServer
func NewServer(locator storage.Locator) gitalypb.TestStreamServiceServer {
	return &server{
		locator: locator,
	}
}
