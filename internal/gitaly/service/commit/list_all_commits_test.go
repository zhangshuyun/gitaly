package commit

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("empty repo", func(t *testing.T) {
		cfg, client := setupCommitService(t)

		repo, _, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
		defer cleanup()

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		require.Empty(t, receiveCommits(t, stream))
	})

	t.Run("normal repo", func(t *testing.T) {
		_, repo, _, client := setupCommitServiceWithRepo(t, true)

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		commits := receiveCommits(t, stream)
		require.Greater(t, len(commits), 350)

		// Build a map of received commits by their OID so that we can easily compare a
		// subset via `testassert.ProtoEqual()`. Ideally, we'd just use `require.Subset()`,
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
			testassert.ProtoEqual(t, gittest.CommitsByID[oid], commitsByID[oid])
		}
	})

	t.Run("pagination", func(t *testing.T) {
		_, repo, _, client := setupCommitServiceWithRepo(t, true)

		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
			PaginationParams: &gitalypb.PaginationParameter{
				PageToken: "1039376155a0d507eba0ea95c29f8f5b983ea34b",
				Limit:     1,
			},
		})
		require.NoError(t, err)

		testassert.ProtoEqual(t, []*gitalypb.GitCommit{
			gittest.CommitsByID["54188278422b1fa877c2e71c4e37fc6640a58ad1"],
		}, receiveCommits(t, stream))
	})

	t.Run("quarantine directory", func(t *testing.T) {
		cfg, repo, repoPath, client := setupCommitServiceWithRepo(t, true)

		quarantineDir := filepath.Join("objects", "incoming-123456")
		require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), 0777))

		repo.GitObjectDirectory = quarantineDir
		repo.GitAlternateObjectDirectories = nil

		// There are no quarantined objects yet, so none should be returned
		// here.
		stream, err := client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		require.Empty(t, receiveCommits(t, stream))

		treeID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "HEAD^{tree}"))

		// We cannot easily spawn a command with an object directory, so we just do so
		// manually here and write the commit into the quarantine object directory.
		commitTree := exec.Command(cfg.Git.BinPath, "-C", repoPath,
			"-c", "user.name=John Doe",
			"-c", "user.email=john.doe@example.com",
			"commit-tree", treeID, "-m", "An empty commit")
		commitTree.Env = []string{
			"GIT_AUTHOR_DATE=1600000000 +0200",
			"GIT_COMMITTER_DATE=1600000001 +0200",
			fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", quarantineDir),
			fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", filepath.Join(repoPath, "objects")),
		}

		commitID, err := commitTree.Output()
		require.NoError(t, err)

		// We now expect only the quarantined commit to be returned.
		stream, err = client.ListAllCommits(ctx, &gitalypb.ListAllCommitsRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		require.Equal(t, []*gitalypb.GitCommit{{
			Id:       text.ChompBytes(commitID),
			Subject:  []byte("An empty commit"),
			Body:     []byte("An empty commit\n"),
			BodySize: 16,
			TreeId:   treeID,
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("John Doe"),
				Email:    []byte("john.doe@example.com"),
				Date:     &timestamp.Timestamp{Seconds: 1600000000},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("John Doe"),
				Email:    []byte("john.doe@example.com"),
				Date:     &timestamp.Timestamp{Seconds: 1600000001},
				Timezone: []byte("+0200"),
			},
		}}, receiveCommits(t, stream))
	})
}
