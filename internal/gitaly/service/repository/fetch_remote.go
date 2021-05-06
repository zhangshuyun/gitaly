package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/errors"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FetchRemote(ctx context.Context, req *gitalypb.FetchRemoteRequest) (*gitalypb.FetchRemoteResponse, error) {
	if err := s.validateFetchRemoteRequest(req); err != nil {
		return nil, err
	}

	var stderr bytes.Buffer
	opts := localrepo.FetchOpts{
		Stderr:  &stderr,
		Force:   req.Force,
		Prune:   !req.NoPrune,
		Tags:    localrepo.FetchOptsTagsAll,
		Verbose: req.GetCheckTagsChanged(),
	}

	if req.GetNoTags() {
		opts.Tags = localrepo.FetchOptsTagsNone
	}

	repo := s.localrepo(req.GetRepository())
	remoteName := req.GetRemote()

	if params := req.GetRemoteParams(); params != nil {
		remoteName = "inmemory"

		remoteURL := params.GetUrl()
		refspecs := s.getRefspecs(params.GetMirrorRefmaps())

		config := []git.ConfigPair{
			{Key: "remote.inmemory.url", Value: remoteURL},
		}

		for _, refspec := range refspecs {
			config = append(config, git.ConfigPair{
				Key: "remote.inmemory.fetch", Value: refspec,
			})
		}

		if authHeader := params.GetHttpAuthorizationHeader(); authHeader != "" {
			config = append(config, git.ConfigPair{
				Key:   fmt.Sprintf("http.%s.extraHeader", remoteURL),
				Value: "Authorization: " + authHeader,
			})
		}

		opts.CommandOptions = append(opts.CommandOptions, git.WithConfigEnv(config...))
	}

	sshCommand, cleanup, err := git.BuildSSHInvocation(ctx, req.GetSshKey(), req.GetKnownHosts())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	opts.Env = append(opts.Env, "GIT_SSH_COMMAND="+sshCommand)

	if req.GetTimeout() > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.GetTimeout())*time.Second)
		defer cancel()
	}

	opts.CommandOptions = append(opts.CommandOptions,
		git.WithConfig(git.ConfigPair{Key: "http.followRedirects", Value: "false"}),
	)

	if err := repo.FetchRemote(ctx, remoteName, opts); err != nil {
		if _, ok := status.FromError(err); ok {
			// this check is used because of internal call to alternates.PathAndEnv
			// which may return gRPC status as an error result
			return nil, err
		}

		errMsg := stderr.String()
		if errMsg != "" {
			return nil, fmt.Errorf("fetch remote: %q: %w", errMsg, err)
		}

		return nil, fmt.Errorf("fetch remote: %w", err)
	}

	// Ideally, we'd do the voting process via git-fetch(1) using the reference-transaction
	// hook. But by default this would lead to one hook invocation per updated ref, which is
	// infeasible performance-wise. While this could be fixed via the `--atomic` flag, that's
	// not a solution either: we rely on the fact that refs get updated even if a subset of refs
	// diverged, and with atomic transactions it would instead be an all-or-nothing operation.
	//
	// Instead, we do the second-best thing, which is to vote on the resulting references. This
	// is of course racy and may conflict with other mutators, causing the vote to fail. But it
	// is arguably preferable to accept races in favour always replicating. If loosing the race,
	// we'd fail this RPC and schedule a replication job afterwards.
	if err := transaction.RunOnContext(ctx, func(tx metadata.Transaction, praefect metadata.PraefectServer) error {
		hash := transaction.NewVoteHash()

		if err := repo.ExecAndWait(ctx, git.SubCmd{
			Name: "for-each-ref",
		}, git.WithStdout(hash)); err != nil {
			return fmt.Errorf("cannot compute references vote: %w", err)
		}

		vote, err := hash.Vote()
		if err != nil {
			return err
		}

		return s.txManager.Vote(ctx, tx, praefect, vote)
	}); err != nil {
		return nil, status.Errorf(codes.Aborted, "failed vote on refs: %v", err)
	}

	out := &gitalypb.FetchRemoteResponse{TagsChanged: true}
	if req.GetCheckTagsChanged() {
		out.TagsChanged = didTagsChange(&stderr)
	}

	return out, nil
}

func didTagsChange(r io.Reader) bool {
	scanner := git.NewFetchScanner(r)
	for scanner.Scan() {
		status := scanner.StatusLine()

		// We can't detect if tags have been deleted, but we never call fetch
		// with --prune-tags at the moment, so it should never happen.
		if status.IsTagAdded() || status.IsTagUpdated() {
			return true
		}
	}

	// If the scanner fails for some reason, we don't know if tags changed, so
	// assume they did for safety reasons.
	return scanner.Err() != nil
}

func (s *server) validateFetchRemoteRequest(req *gitalypb.FetchRemoteRequest) error {
	if req.GetRepository() == nil {
		return helper.ErrInvalidArgument(errors.ErrEmptyRepository)
	}

	params := req.GetRemoteParams()
	if params == nil {
		remote := req.GetRemote()
		if strings.TrimSpace(remote) == "" {
			return helper.ErrInvalidArgument(fmt.Errorf(`blank or empty "remote": %q`, remote))
		}
		return nil
	}

	remoteURL, err := url.ParseRequestURI(params.GetUrl())
	if err != nil {
		return helper.ErrInvalidArgument(fmt.Errorf(`invalid "remote_params.url": %q: %w`, params.GetUrl(), err))
	}

	if remoteURL.Host == "" {
		return helper.ErrInvalidArgumentf(`invalid "remote_params.url": %q: no host`, params.GetUrl())
	}

	return nil
}

func (s *server) getRefspecs(refmaps []string) []string {
	if len(refmaps) == 0 {
		return []string{"refs/*:refs/*"}
	}

	refspecs := make([]string, 0, len(refmaps))

	for _, refmap := range refmaps {
		switch refmap {
		case "all_refs":
			// with `all_refs`, the repository is equivalent to the result of `git clone --mirror`
			refspecs = append(refspecs, "refs/*:refs/*")
		case "heads":
			refspecs = append(refspecs, "refs/heads/*:refs/heads/*")
		case "tags":
			refspecs = append(refspecs, "refs/tags/*:refs/tags/*")
		default:
			refspecs = append(refspecs, refmap)
		}
	}
	return refspecs
}
