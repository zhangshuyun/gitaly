package commit

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

type server struct {
	locator storage.Locator
}

var (
	defaultBranchName = ref.DefaultBranchName
)

// NewServer creates a new instance of a grpc CommitServiceServer
func NewServer(locator storage.Locator) gitalypb.CommitServiceServer {
	return &server{locator: locator}
}
