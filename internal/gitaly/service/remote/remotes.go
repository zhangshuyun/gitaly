package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AddRemote adds a remote to the repository
func (s *server) AddRemote(ctx context.Context, req *gitalypb.AddRemoteRequest) (*gitalypb.AddRemoteResponse, error) {
	if err := validateAddRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "AddRemote: %v", err)
	}

	client, err := s.ruby.RemoteServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}

	if err := s.voteOnRemote(ctx, req.GetRepository(), req.GetName()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on remote: %v", err)
	}

	response, err := client.AddRemote(clientCtx, req)
	if err != nil {
		return nil, err
	}

	if err := s.voteOnRemote(ctx, req.GetRepository(), req.GetName()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on remote: %v", err)
	}

	return response, nil
}

func validateAddRemoteRequest(req *gitalypb.AddRemoteRequest) error {
	if strings.TrimSpace(req.GetName()) == "" {
		return fmt.Errorf("empty remote name")
	}
	if req.GetUrl() == "" {
		return fmt.Errorf("empty remote url")
	}

	return nil
}

// RemoveRemote removes the given remote
func (s *server) RemoveRemote(ctx context.Context, req *gitalypb.RemoveRemoteRequest) (*gitalypb.RemoveRemoteResponse, error) {
	if err := validateRemoveRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RemoveRemote: %v", err)
	}

	remote := s.localrepo(req.GetRepository()).Remote()

	hasRemote, err := remote.Exists(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	if !hasRemote {
		return &gitalypb.RemoveRemoteResponse{Result: false}, nil
	}

	if err := s.voteOnRemote(ctx, req.GetRepository(), req.GetName()); err != nil {
		return nil, helper.ErrInternalf("preimage vote on remote: %v", err)
	}

	if err := remote.Remove(ctx, req.Name); err != nil {
		return nil, err
	}

	if err := s.voteOnRemote(ctx, req.GetRepository(), req.GetName()); err != nil {
		return nil, helper.ErrInternalf("postimage vote on remote: %v", err)
	}

	return &gitalypb.RemoveRemoteResponse{Result: true}, nil
}

func (s *server) FindRemoteRepository(ctx context.Context, req *gitalypb.FindRemoteRepositoryRequest) (*gitalypb.FindRemoteRepositoryResponse, error) {
	if req.GetRemote() == "" {
		return nil, status.Error(codes.InvalidArgument, "FindRemoteRepository: empty remote can't be checked.")
	}

	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "ls-remote",
			Args: []string{
				req.GetRemote(),
				"HEAD",
			},
		},
	)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error executing git command: %s", err)
	}

	output, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to read stdout: %s", err)
	}
	if err := cmd.Wait(); err != nil {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// The output of a successful command is structured like
	// Regexp would've read better, but this is faster
	// 58fbff2e0d3b620f591a748c158799ead87b51cd	HEAD
	fields := bytes.Fields(output)
	match := len(fields) == 2 && len(fields[0]) == 40 && string(fields[1]) == "HEAD"

	return &gitalypb.FindRemoteRepositoryResponse{Exists: match}, nil
}

func validateRemoveRemoteRequest(req *gitalypb.RemoveRemoteRequest) error {
	if req.GetName() == "" {
		return fmt.Errorf("empty remote name")
	}

	return nil
}

func (s *server) voteOnRemote(ctx context.Context, repo *gitalypb.Repository, remoteName string) error {
	if featureflag.IsDisabled(ctx, featureflag.TxRemote) {
		return nil
	}

	return transaction.RunOnContext(ctx, func(tx txinfo.Transaction, server txinfo.PraefectServer) error {
		localrepo := s.localrepo(repo)

		configEntries, err := localrepo.Config().GetRegexp(ctx, "remote\\."+remoteName+"\\.", git.ConfigGetRegexpOpts{})
		if err != nil {
			return fmt.Errorf("get remote configuration: %w", err)
		}

		hash := voting.NewVoteHash()
		for _, configEntry := range configEntries {
			config := fmt.Sprintf("%s\t%s\n", configEntry.Key, configEntry.Value)
			if _, err := io.WriteString(hash, config); err != nil {
				return fmt.Errorf("hash remote config entry: %w", err)
			}
		}

		vote, err := hash.Vote()
		if err != nil {
			return fmt.Errorf("compute remote config vote: %w", err)
		}

		if err := s.txManager.Vote(ctx, tx, server, vote); err != nil {
			return fmt.Errorf("vote: %w", err)
		}

		return nil
	})
}
