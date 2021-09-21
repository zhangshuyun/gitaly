package repository

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
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

func (s *server) setConfigGit2Go(ctx context.Context, req *gitalypb.SetConfigRequest) (*gitalypb.SetConfigResponse, error) {
	reqRepo := req.GetRepository()
	if reqRepo == nil {
		return nil, status.Error(codes.InvalidArgument, "no repository")
	}

	path, err := s.locator.GetRepoPath(reqRepo)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]git2go.ConfigEntry)

	for _, el := range req.Entries {
		switch el.GetValue().(type) {
		case *gitalypb.SetConfigRequest_Entry_ValueStr:
			entries[el.Key] = git2go.ConfigEntry{Value: el.GetValueStr()}
		case *gitalypb.SetConfigRequest_Entry_ValueInt32:
			entries[el.Key] = git2go.ConfigEntry{Value: el.GetValueInt32()}
		case *gitalypb.SetConfigRequest_Entry_ValueBool:
			entries[el.Key] = git2go.ConfigEntry{Value: el.GetValueBool()}
		default:
			return nil, status.Error(codes.InvalidArgument, "unknown entry type")
		}
	}

	/*
	 * We're voting twice, once on the preimage and once on the postimage. Please refer to the
	 * comment in DeleteConfig() for the reason.
	 */
	if err := s.voteOnConfig(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on config: %v", err)
	}

	if err := s.git2go.SetConfig(ctx, reqRepo, git2go.SetConfigCommand{Repository: path, Entries: entries}); err != nil {
		return nil, status.Errorf(codes.Internal, "SetConfig git2go error")
	}

	if err := s.voteOnConfig(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on config: %v", err)
	}

	return &gitalypb.SetConfigResponse{}, nil
}

func (s *server) SetConfig(ctx context.Context, req *gitalypb.SetConfigRequest) (*gitalypb.SetConfigResponse, error) {
	// We use gitaly-ruby here because in gitaly-ruby we can use Rugged, and
	// Rugged lets us set config values without leaking secrets via 'ps'. We
	// can't use `git config foo.bar secret` because that leaks secrets.
	// Also we can use git2go implementation of SetConfig

	if featureflag.GoSetConfig.IsEnabled(ctx) {
		return s.setConfigGit2Go(ctx, req)
	}

	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	/*
	 * We're voting twice, once on the preimage and once on the postimage. Please refer to the
	 * comment in DeleteConfig() for the reason.
	 */
	if err := s.voteOnConfig(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on config: %v", err)
	}

	//nolint:staticcheck
	response, err := client.SetConfig(clientCtx, req)
	if err != nil {
		return nil, err
	}

	if err := s.voteOnConfig(ctx, req.GetRepository()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on config: %v", err)
	}

	return response, nil
}

func (s *server) voteOnConfig(ctx context.Context, repo *gitalypb.Repository) error {
	return transaction.RunOnContext(ctx, func(tx txinfo.Transaction) error {
		repoPath, err := s.locator.GetPath(repo)
		if err != nil {
			return fmt.Errorf("get repo path: %w", err)
		}

		var vote voting.Vote

		config, err := os.Open(filepath.Join(repoPath, "config"))
		switch {
		case err == nil:
			hash := voting.NewVoteHash()
			if _, err := io.Copy(hash, config); err != nil {
				return fmt.Errorf("seeding vote: %w", err)
			}

			vote, err = hash.Vote()
			if err != nil {
				return fmt.Errorf("computing vote: %w", err)
			}
		case os.IsNotExist(err):
			vote = voting.VoteFromData([]byte("notfound"))
		default:
			return fmt.Errorf("open repo config: %w", err)
		}

		if err := s.txManager.Vote(ctx, tx, vote); err != nil {
			return fmt.Errorf("casting vote: %w", err)
		}

		return nil
	})
}
