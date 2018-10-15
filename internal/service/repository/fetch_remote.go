package repository

import (
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
)

func (s *server) FetchRemote(ctx context.Context, in *gitalypb.FetchRemoteRequest) (*gitalypb.FetchRemoteResponse, error) {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"Remote":     in.GetRemote(),
		"Force":      in.GetForce(),
		"NoTags":     in.GetNoTags(),
		"Timeout":    in.GetTimeout(),
		"SSHKey":     in.GetSshKey(),
		"KnownHosts": in.GetKnownHosts(),
	}).Debug("FetchRemote")

	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.FetchRemote(clientCtx, in)
}
