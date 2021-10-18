package repository

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CalculateChecksum(ctx context.Context, in *gitalypb.CalculateChecksumRequest) (*gitalypb.CalculateChecksumResponse, error) {
	repo := in.GetRepository()

	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	cmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{Name: "show-ref", Flags: []git.Option{git.Flag{Name: "--head"}}})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		return nil, status.Errorf(codes.Internal, "CalculateChecksum: gitCommand: %v", err)
	}

	var checksum git.Checksum

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		checksum.AddBytes(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	if err := cmd.Wait(); checksum.IsZero() || err != nil {
		if s.isValidRepo(ctx, repo) {
			return &gitalypb.CalculateChecksumResponse{Checksum: git.ZeroOID.String()}, nil
		}

		return nil, status.Errorf(codes.DataLoss, "CalculateChecksum: not a git repository '%s'", repoPath)
	}

	return &gitalypb.CalculateChecksumResponse{Checksum: hex.EncodeToString(checksum.Bytes())}, nil
}

func (s *server) isValidRepo(ctx context.Context, repo *gitalypb.Repository) bool {
	stdout := &bytes.Buffer{}
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubCmd{
			Name: "rev-parse",
			Flags: []git.Option{
				git.Flag{Name: "--is-bare-repository"},
			},
		},
		git.WithStdout(stdout),
	)
	if err != nil {
		return false
	}

	if err := cmd.Wait(); err != nil {
		return false
	}

	return strings.EqualFold(strings.TrimRight(stdout.String(), "\n"), "true")
}
