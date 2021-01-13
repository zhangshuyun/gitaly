package repository

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (*server) DeleteConfig(ctx context.Context, req *gitalypb.DeleteConfigRequest) (*gitalypb.DeleteConfigResponse, error) {
	for _, k := range req.Keys {
		// We assume k does not contain any secrets; it is leaked via 'ps'.
		cmd, err := git.NewCommand(ctx, req.Repository, nil, git.SubCmd{
			Name:  "config",
			Flags: []git.Option{git.ValueFlag{"--unset-all", k}},
		})
		if err != nil {
			return nil, err
		}

		if err := cmd.Wait(); err != nil {
			if code, ok := command.ExitStatus(err); ok && code == 5 {
				// Status code 5 means 'key not in config', see 'git help config'
				continue
			}

			return nil, status.Errorf(codes.Internal, "command failed: %v", err)
		}
	}

	return &gitalypb.DeleteConfigResponse{}, nil
}

func (s *server) SetConfig(ctx context.Context, req *gitalypb.SetConfigRequest) (*gitalypb.SetConfigResponse, error) {
	// We use gitaly-ruby here because in gitaly-ruby we can use Rugged, and
	// Rugged lets us set config values without leaking secrets via 'ps'. We
	// can't use `git config foo.bar secret` because that leaks secrets.
	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.SetConfig(clientCtx, req)
}
