package hook

import (
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type server struct {
	internalClient *internalClient
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(c config.Cfg) (gitalypb.HookServiceServer, error) {
	client, err := newInternalClient(
		c.GitlabURL,
		c.GitlabShell.SecretFile,
		c.HTTP.SelfSigned,
		c.HTTP.CAFile,
		c.HTTP.CAPath,
	)
	if err != nil {
		return nil, err
	}

	return &server{
		internalClient: client,
	}, nil
}
