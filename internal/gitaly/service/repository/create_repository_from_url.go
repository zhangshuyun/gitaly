package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) cloneFromURLCommand(
	ctx context.Context,
	repoURL, repoHost, repositoryFullPath, authorizationToken string,
	opts ...git.CmdOpt,
) (*command.Command, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	var config []git.ConfigPair

	cloneFlags := []git.Option{
		git.Flag{Name: "--mirror"},
		git.Flag{Name: "--quiet"},
	}

	if u.User != nil {
		pwd, set := u.User.Password()

		var creds string
		if set {
			creds = u.User.Username() + ":" + pwd
		} else {
			creds = u.User.Username()
		}

		u.User = nil
		authHeader := fmt.Sprintf("Authorization: Basic %s", base64.StdEncoding.EncodeToString([]byte(creds)))
		config = append(config, git.ConfigPair{Key: "http.extraHeader", Value: authHeader})
	} else {
		if len(authorizationToken) > 0 {
			authHeader := fmt.Sprintf("Authorization: %s", authorizationToken)
			config = append(config, git.ConfigPair{Key: "http.extraHeader", Value: authHeader})
		}
	}

	if repoHost != "" {
		config = append(config, git.ConfigPair{
			Key:   "http.extraHeader",
			Value: "Host: " + repoHost,
		})
	}

	return s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name:  "clone",
			Flags: cloneFlags,
			Args:  []string{u.String(), repositoryFullPath},
		},
		append(opts, git.WithConfig(config...))...,
	)
}

func (s *server) CreateRepositoryFromURL(ctx context.Context, req *gitalypb.CreateRepositoryFromURLRequest) (*gitalypb.CreateRepositoryFromURLResponse, error) {
	if err := validateCreateRepositoryFromURLRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateRepositoryFromURL: %v", err)
	}

	if err := s.createRepository(ctx, req.GetRepository(), func(repo *gitalypb.Repository) error {
		targetPath, err := s.locator.GetPath(repo)
		if err != nil {
			return fmt.Errorf("getting temporary repository path: %w", err)
		}

		// We need to remove the target path first so git-clone(1) doesn't complain.
		if err := os.RemoveAll(targetPath); err != nil {
			return fmt.Errorf("removing temporary repository: %w", err)
		}

		var stderr bytes.Buffer
		cmd, err := s.cloneFromURLCommand(ctx,
			req.GetUrl(),
			req.GetHttpHost(),
			targetPath,
			req.GetHttpAuthorizationHeader(),
			git.WithStderr(&stderr),
			git.WithDisabledHooks(),
		)
		if err != nil {
			return fmt.Errorf("starting clone: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("cloning repository: %w, stderr: %q", err, stderr.String())
		}

		if err := s.removeOriginInRepo(ctx, repo); err != nil {
			return fmt.Errorf("removing origin remote: %w", err)
		}

		return nil
	}); err != nil {
		return nil, helper.ErrInternalf("creating repository: %w", err)
	}

	return &gitalypb.CreateRepositoryFromURLResponse{}, nil
}

func validateCreateRepositoryFromURLRequest(req *gitalypb.CreateRepositoryFromURLRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetUrl() == "" {
		return fmt.Errorf("empty Url")
	}

	return nil
}
