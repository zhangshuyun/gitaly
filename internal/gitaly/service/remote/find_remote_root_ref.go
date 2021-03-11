package remote

import (
	"bufio"
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const headPrefix = "HEAD branch: "

func (s *server) findRemoteRootRef(ctx context.Context, repo *gitalypb.Repository, remote string) (string, error) {
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "remote",
			Action: "show",
			Args:   []string{remote},
		},
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, headPrefix) {
			return strings.TrimPrefix(line, headPrefix), nil
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
	remote := in.GetRemote()
	if remote == "" {
		return nil, status.Error(codes.InvalidArgument, "empty remote can't be queried")
	}

	ref, err := s.findRemoteRootRef(ctx, in.GetRepository(), remote)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}

	return &gitalypb.FindRemoteRootRefResponse{Ref: ref}, nil
}
