package wiki

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type server struct {
	gitalypb.UnimplementedWikiServiceServer
	ruby    *rubyserver.Server
	locator storage.Locator
}

// NewServer creates a new instance of a grpc WikiServiceServer
func NewServer(rs *rubyserver.Server, locator storage.Locator) gitalypb.WikiServiceServer {
	return &server{ruby: rs, locator: locator}
}
