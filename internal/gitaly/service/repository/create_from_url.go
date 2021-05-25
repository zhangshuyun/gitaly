package repository

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) cloneFromURLCommand(ctx context.Context, repo *gitalypb.Repository, repoURL, repositoryFullPath string, stderr io.Writer) (*command.Command, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	config := []git.ConfigPair{
		{Key: "http.followRedirects", Value: "false"},
	}

	cloneFlags := []git.Option{
		git.Flag{Name: "--bare"},
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
	}

	return s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name:        "clone",
			Flags:       cloneFlags,
			PostSepArgs: []string{u.String(), repositoryFullPath},
		},
		git.WithStderr(stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
		git.WithConfig(config...),
	)
}

func (s *server) CreateRepositoryFromURL(ctx context.Context, req *gitalypb.CreateRepositoryFromURLRequest) (*gitalypb.CreateRepositoryFromURLResponse, error) {
	if err := validateCreateRepositoryFromURLRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateRepositoryFromURL: %v", err)
	}

	repository := req.Repository

	repositoryFullPath, err := s.locator.GetPath(repository)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(repositoryFullPath); !os.IsNotExist(err) {
		return nil, status.Errorf(codes.InvalidArgument, "CreateRepositoryFromURL: dest dir exists")
	}

	stderr := bytes.Buffer{}
	cmd, err := s.cloneFromURLCommand(ctx, repository, req.GetUrl(), repositoryFullPath, &stderr)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		os.RemoveAll(repositoryFullPath)
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: clone cmd wait: %s: %v", stderr.String(), err)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repository}); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: create hooks failed: %v", err)
	}

	if err := s.removeOriginInRepo(ctx, repository); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateRepositoryFromURL: %v", err)
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
