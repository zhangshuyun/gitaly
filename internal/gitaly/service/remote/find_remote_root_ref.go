package remote

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const headPrefix = "HEAD branch: "

func (s *server) findRemoteRootRef(ctx context.Context, request *gitalypb.FindRemoteRootRefRequest) (string, error) {
	remoteName := request.Remote
	var config []git.ConfigPair

	if request.RemoteUrl != "" {
		remoteName = "inmemory"
		config = []git.ConfigPair{
			{Key: "remote.inmemory.url", Value: request.RemoteUrl},
		}

		if authHeader := request.GetHttpAuthorizationHeader(); authHeader != "" {
			config = append(config, git.ConfigPair{
				Key:   fmt.Sprintf("http.%s.extraHeader", request.RemoteUrl),
				Value: "Authorization: " + authHeader,
			})
		}
	}

	cmd, err := s.gitCmdFactory.New(ctx, request.Repository,
		git.SubSubCmd{
			Name:   "remote",
			Action: "show",
			Args:   []string{remoteName},
		},
		git.WithRefTxHook(ctx, request.Repository, s.cfg),
		git.WithConfigEnv(config...),
	)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, headPrefix) {
			rootRef := strings.TrimPrefix(line, headPrefix)
			if rootRef == "(unknown)" {
				return "", status.Error(codes.NotFound, "no remote HEAD found")
			}
			return rootRef, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", err
	}

	return "", status.Error(codes.NotFound, "couldn't query the remote HEAD")
}

// FindRemoteRootRef queries the remote to determine its HEAD
func (s *server) FindRemoteRootRef(ctx context.Context, in *gitalypb.FindRemoteRootRefRequest) (*gitalypb.FindRemoteRootRefResponse, error) {
	//nolint:staticcheck // GetRemote() is deprecated
	if in.GetRemote() == "" && in.GetRemoteUrl() == "" {
		return nil, status.Error(codes.InvalidArgument, "got neither remote name nor URL")
	}
	//nolint:staticcheck // GetRemote() is deprecated
	if in.GetRemote() != "" && in.GetRemoteUrl() != "" {
		return nil, status.Error(codes.InvalidArgument, "got remote name and URL")
	}
	if in.Repository == nil {
		return nil, status.Error(codes.InvalidArgument, "missing repository")
	}

	ref, err := s.findRemoteRootRef(ctx, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FindRemoteRootRefResponse{Ref: ref}, nil
}
