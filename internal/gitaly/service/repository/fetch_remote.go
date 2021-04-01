package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/errors"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	repo := localrepo.New(s.gitCmdFactory, req.GetRepository(), s.cfg)
	remoteName := req.GetRemote()

	if params := req.GetRemoteParams(); params != nil {
		gitVersion, err := git.CurrentVersion(ctx, s.gitCmdFactory)
		if err != nil {
			return nil, fmt.Errorf("cannot determine current git version: %w", err)
		}

		remoteURL := params.GetUrl()
		refspecs := s.getRefspecs(params.GetMirrorRefmaps())

		if gitVersion.SupportsConfigEnv() {
			remoteName = "inmemory"

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
		} else {
			remoteSuffix, err := text.RandomHex(16)
			if err != nil {
				return nil, fmt.Errorf("cannot generate remote suffix: %w", err)
			}

			// We're generating a random name for the remote. We do not use a static name here
			// and neither do we use a user-provided one as we'll be removing the remote after
			// this RPC again, which also removes all its contained references.
			//
			// Ideally, we'd just use an in-memory remote either by fetching via URL directly or
			// by injecting an in-memory remote via git configuration. But the former is not
			// possible as it may leak credentials stored in URLs, while the latter is not
			// possible yet because git has no official way to inject configuration via the
			// environment yet. That is about to change though with git v2.31.0, so we can
			// migrate this to use anonymous remotes as soon as we require that as minimum
			// version.
			remoteName = fmt.Sprintf("tmp-%s", remoteSuffix)

			if err := s.setRemote(ctx, repo, remoteName, remoteURL); err != nil {
				return nil, fmt.Errorf("set remote: %w", err)
			}

			defer func(parentCtx context.Context) {
				ctx, cancel := context.WithCancel(command.SuppressCancellation(parentCtx))
				defer cancel()

				// we pass context as it may be overridden in case timeout is set for the call
				if err := s.removeRemote(ctx, repo, remoteName); err != nil {
					ctxlogrus.Extract(ctx).WithError(err).WithFields(logrus.Fields{
						"remote":  remoteName,
						"storage": req.GetRepository().GetStorageName(),
						"path":    req.GetRepository().GetRelativePath(),
					}).Error("removal of remote failed")
				}
			}(ctx)

			for _, refspec := range refspecs {
				opts.CommandOptions = append(opts.CommandOptions,
					git.WithConfig(git.ConfigPair{Key: "remote." + remoteName + ".fetch", Value: refspec}),
				)
			}

			if params.GetHttpAuthorizationHeader() != "" {
				client, err := s.ruby.RepositoryServiceClient(ctx)
				if err != nil {
					return nil, err
				}

				clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
				if err != nil {
					return nil, err
				}

				// currently it is only possible way to set config value without exposing it to outside (won't be listed in 'ps')
				extraHeaderKey := "http." + remoteURL + ".extraHeader"

				if _, err := client.SetConfig(clientCtx, &gitalypb.SetConfigRequest{
					Repository: req.GetRepository(),
					Entries: []*gitalypb.SetConfigRequest_Entry{{
						Key:   extraHeaderKey,
						Value: &gitalypb.SetConfigRequest_Entry_ValueStr{ValueStr: "Authorization: " + params.GetHttpAuthorizationHeader()},
					}},
				}); err != nil {
					return nil, helper.ErrInternal(fmt.Errorf("set extra header: %w", err))
				}

				defer func() {
					ctx, cancel := context.WithCancel(command.SuppressCancellation(clientCtx))
					defer cancel()

					// currently it is only possible way to set config value without exposing it to outside (won't be listed in 'ps')
					if _, err := client.DeleteConfig(ctx, &gitalypb.DeleteConfigRequest{
						Repository: req.Repository,
						Keys:       []string{extraHeaderKey},
					}); err != nil {
						ctxlogrus.Extract(ctx).WithError(err).WithFields(logrus.Fields{
							"remote":  remoteName,
							"storage": req.GetRepository().GetStorageName(),
							"path":    req.GetRepository().GetRelativePath(),
						}).Error("removal of extra header config failed")
					}
				}()
			}
		}
	} else {
		sshCommand, cleanup, err := git.BuildSSHInvocation(ctx, req.GetSshKey(), req.GetKnownHosts())
		if err != nil {
			return nil, err
		}
		defer cleanup()

		opts.Env = append(opts.Env, "GIT_SSH_COMMAND="+sshCommand)
	}

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

func (s *server) setRemote(ctx context.Context, repo *localrepo.Repo, name, url string) error {
	if err := repo.Remote().Remove(ctx, name); err != nil {
		if err != git.ErrNotFound {
			return fmt.Errorf("remove remote: %w", err)
		}
	}

	if err := repo.Remote().Add(ctx, name, url, git.RemoteAddOpts{}); err != nil {
		return fmt.Errorf("add remote: %w", err)
	}

	return nil
}

func (s *server) removeRemote(ctx context.Context, repo *localrepo.Repo, name string) error {
	if err := repo.Remote().Remove(ctx, name); err != nil {
		if err != git.ErrNotFound {
			return fmt.Errorf("remove remote: %w", err)
		}
	}

	return nil
}
