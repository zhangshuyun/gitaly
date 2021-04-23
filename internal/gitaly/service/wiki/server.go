package wiki

import (
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
