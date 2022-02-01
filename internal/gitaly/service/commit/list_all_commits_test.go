package commit

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListAllCommits(t *testing.T) {
	receiveCommits := func(t *testing.T, stream gitalypb.CommitService_ListAllCommitsClient) []*gitalypb.GitCommit {
		t.Helper()

		var commits []*gitalypb.GitCommit
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)

			commits = append(commits, response.Commits...)
		}

		return commits
	}
	ctx := testhelper.Context(t)

	t.Run("empty repo", func(t *testing.T) {
		cfg, client := setupCommitService(ctx, t)

		repo, _ := gittest.CreateRepository(ctx, t, cfg)

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		require.Empty(t, receiveCommits(t, stream))
	})

	t.Run("normal repo", func(t *testing.T) {
		_, repo, _, client := setupCommitServiceWithRepo(ctx, t, true)

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		commits := receiveCommits(t, stream)
		require.Greater(t, len(commits), 350)

		// Build a map of received commits by their OID so that we can easily compare a
		// subset via `testhelper.ProtoEqual()`. Ideally, we'd just use `require.Subset()`,
		// but that doesn't work with protobuf messages.
		commitsByID := make(map[string]*gitalypb.GitCommit)
		for _, commit := range commits {
			commitsByID[commit.Id] = commit
		}

		// We've got quite a bunch of commits, so let's only compare a small subset to be
		// sure that commits are correctly read.
		for _, oid := range []string{
			"0031876facac3f2b2702a0e53a26e89939a42209",
			"48ca272b947f49eee601639d743784a176574a09",
			"335bc94d5b7369b10251e612158da2e4a4aaa2a5",
			"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
		} {
			testhelper.ProtoEqual(t, gittest.CommitsByID[oid], commitsByID[oid])
		}
	})

	t.Run("pagination", func(t *testing.T) {
		_, repo, _, client := setupCommitServiceWithRepo(ctx, t, true)

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "1039376155a0d507eba0ea95c29f8f5b983ea34b",
				Limit:     1,
			},
		})
		require.NoError(t, err)

		testhelper.ProtoEqual(t, []*gitalypb.GitCommit{
			gittest.CommitsByID["54188278422b1fa877c2e71c4e37fc6640a58ad1"],
		}, receiveCommits(t, stream))
	})

	t.Run("quarantine directory", func(t *testing.T) {
		cfg, repo, repoPath, client := setupCommitServiceWithRepo(ctx, t, true)

		quarantineDir := filepath.Join("objects", "incoming-123456")
		require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), 0o777))

		repo.GitObjectDirectory = quarantineDir
		repo.GitAlternateObjectDirectories = nil

		// There are no quarantined objects yet, so none should be returned
		// here.
		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		require.Empty(t, receiveCommits(t, stream))

		// We cannot easily spawn a command with an object directory, so we just do so
		// manually here and write the commit into the quarantine object directory.
		commitID := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithAlternateObjectDirectory(filepath.Join(repoPath, quarantineDir)),
			gittest.WithParents(),
		)

		// We now expect only the quarantined commit to be returned.
		stream, err = client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		require.Equal(t, []*gitalypb.GitCommit{{
			Id:       commitID.String(),
			Subject:  []byte("message"),
			Body:     []byte("message"),
			BodySize: 7,
			TreeId:   "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Scrooge McDuck"),
				Email:    []byte("scrooge@mcduck.com"),
				Date:     &timestamppb.Timestamp{Seconds: 1572776879},
				Timezone: []byte("+0100"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Scrooge McDuck"),
				Email:    []byte("scrooge@mcduck.com"),
				Date:     &timestamppb.Timestamp{Seconds: 1572776879},
				Timezone: []byte("+0100"),
			},
		}}, receiveCommits(t, stream))
	})
}

func BenchmarkListAllCommits(b *testing.B) {
	b.StopTimer()
	ctx := testhelper.Context(b)

	_, repo, _, client := setupCommitServiceWithRepo(ctx, b, true)

	b.Run("ListAllCommits", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
				Repository: repo,
			})
			require.NoError(b, err)

			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(b, err)
			}
		}
	})
}
