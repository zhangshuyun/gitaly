package blob

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	ruby          *rubyserver.Server
	cfg           config.Cfg
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
}

// NewServer creates a new instance of a grpc BlobServer
func NewServer(rs *rubyserver.Server, cfg config.Cfg, locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.BlobServiceServer {
	return &server{
		ruby:          rs,
		cfg:           cfg,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
	}
}
