package repository

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetConfig reads the repository's gitconfig file and returns its contents.
func (s *server) GetConfig(
	request *gitalypb.GetConfigRequest,
	stream gitalypb.RepositoryService_GetConfigServer,
) error {
	repoPath, err := s.locator.GetPath(request.GetRepository())
	if err != nil {
		return err
	}

	configPath := filepath.Join(repoPath, "config")

	gitconfig, err := os.Open(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return status.Errorf(codes.NotFound, "opening gitconfig: %v", err)
		}
		return helper.ErrInternalf("opening gitconfig: %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetConfigResponse{
			Data: p,
		})
	})

	if _, err := io.Copy(writer, gitconfig); err != nil {
		return helper.ErrInternalf("sending config: %v", err)
	}

	return nil
}

func (s *server) DeleteConfig(ctx context.Context, req *gitalypb.DeleteConfigRequest) (*gitalypb.DeleteConfigResponse, error) {
	for _, k := range req.Keys {
		// We assume k does not contain any secrets; it is leaked via 'ps'.
		cmd, err := s.gitCmdFactory.New(ctx, req.Repository, git.SubCmd{
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
