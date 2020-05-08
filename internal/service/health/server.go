package health

import (
	"gitlab.com/gitlab-org/gitaly/internal/middleware/errorhandler"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	errorTracker *errorhandler.Errors
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(errorTracker *errorhandler.Errors) gitalypb.HealthServer {
	return &server{errorTracker: errorTracker}
}
