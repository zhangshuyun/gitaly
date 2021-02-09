package diff

import (
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const msgSizeThreshold = 5 * 1024

type server struct {
	MsgSizeThreshold int
	locator          storage.Locator
	gitCmdFactory    git.CommandFactory
}

// NewServer creates a new instance of a gRPC DiffServer
func NewServer(locator storage.Locator, gitCmdFactory git.CommandFactory) gitalypb.DiffServiceServer {
	return &server{
		MsgSizeThreshold: msgSizeThreshold,
		locator:          locator,
		gitCmdFactory:    gitCmdFactory,
	}
}
