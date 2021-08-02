package repository

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var refWhitelist = regexp.MustCompile(`HEAD|(refs/(heads|tags|keep-around|merge-requests|environments|notes)/)`)

func (s *server) CalculateChecksum(ctx context.Context, in *gitalypb.CalculateChecksumRequest) (*gitalypb.CalculateChecksumResponse, error) {
	repo := s.localrepo(in.GetRepository())

	refs, err := repo.GetReferences(ctx, true)
	if len(refs) == 0 || err != nil {
		if s.isValidRepo(ctx, in.GetRepository()) {
			return &gitalypb.CalculateChecksumResponse{Checksum: git.ZeroOID.String()}, nil
		}

		if _, ok := status.FromError(err); ok {
			return nil, err
		}

		repoPath, err := s.locator.GetRepoPath(repo)
		if err != nil {
			return nil, err
		}

		return nil, status.Errorf(codes.DataLoss, "CalculateChecksum: not a git repository '%s'", repoPath)
	}

	var checksum *big.Int

	for _, ref := range refs {
		if !refWhitelist.MatchString(ref.Name.String()) {
			continue
		}

		h := sha1.New()
		// hash.Hash will never return an error.
		_, _ = fmt.Fprintf(h, "%s %s", ref.Target, ref.Name)

		hash := hex.EncodeToString(h.Sum(nil))
		hashIntBase16, _ := (&big.Int{}).SetString(hash, 16)

		if checksum == nil {
			checksum = hashIntBase16
		} else {
			checksum.Xor(checksum, hashIntBase16)
		}
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
