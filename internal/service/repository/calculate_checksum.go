package repository

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"math/big"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const blankChecksum = "0000000000000000000000000000000000000000"

var refWhitelist = regexp.MustCompile(`HEAD|(refs/(heads|tags|keep-around|merge-requests|environments|notes)/)`)

func (s *server) CalculateChecksum(ctx context.Context, in *gitalypb.CalculateChecksumRequest) (*gitalypb.CalculateChecksumResponse, error) {
	repo := in.GetRepository()

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	args := []string{
		"show-ref",
		"--head",
	}

	cmd, err := git.Command(ctx, repo, args...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		return nil, status.Errorf(codes.Internal, "CalculateChecksum: gitCommand: %v", err)
	}

	var checksum *big.Int

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		ref := scanner.Bytes()

		if !refWhitelist.Match(ref) {
			continue
		}

		h := sha1.New()
		h.Write(ref)

		hash := hex.EncodeToString(h.Sum(nil))
		hashIntBase16, _ := new(big.Int).SetString(hash, 16)

		if checksum == nil {
			checksum = hashIntBase16
		} else {
			checksum.Xor(checksum, hashIntBase16)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}

	if err := cmd.Wait(); checksum == nil || err != nil {
		if isValidRepo(ctx, repo) {
			return &gitalypb.CalculateChecksumResponse{Checksum: blankChecksum}, nil
		}

		return nil, status.Errorf(codes.DataLoss, "CalculateChecksum: not a git repository '%s'", repoPath)
	}

	return &gitalypb.CalculateChecksumResponse{Checksum: hex.EncodeToString(checksum.Bytes())}, nil
}

func isValidRepo(ctx context.Context, repo *gitalypb.Repository) bool {
	repoPath, env, err := alternates.PathAndEnv(repo)
	if err != nil {
		return false
	}

	args := []string{"-C", repoPath, "rev-parse", "--is-inside-git-dir"}
	stdout := &bytes.Buffer{}
	cmd, err := git.BareCommand(ctx, nil, stdout, nil, env, args...)
	if err != nil {
		return false
	}

	if err := cmd.Wait(); err != nil {
		return false
	}

	return strings.EqualFold(strings.TrimRight(stdout.String(), "\n"), "true")
}
