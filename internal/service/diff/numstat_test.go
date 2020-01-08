package diff

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulDiffStatsRequest(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.DiffStatsRequest{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	ctx, cancel := testhelper.Context()
	defer cancel()

	expectedStats := []diff.NumStat{
		{
			Path:      []byte("CONTRIBUTING.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("MAINTENANCE.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("README.md"),
			Additions: 1,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/deleted-file"),
			Additions: 0,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/file-with-multiple-chunks"),
			Additions: 28,
			Deletions: 23,
		},
		{
			Path:      []byte("gitaly/logo-white.png"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/mode-file"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/mode-file-with-mods"),
			Additions: 2,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/named-file-with-mods"),
			Additions: 0,
			Deletions: 1,
		},
		{
			Path:      []byte("gitaly/no-newline-at-the-end"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/renamed-file"),
			Additions: 0,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/renamed-file-with-mods"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/tab\tnewline\n file"),
			Additions: 1,
			Deletions: 0,
		},
		{
			Path:      []byte("gitaly/テスト.txt"),
			Additions: 0,
			Deletions: 0,
		},
	}

	stream, err := client.DiffStats(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	for {
		fetchedStats, err := stream.Recv()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		stats := fetchedStats.GetStats()

		for index, fetchedStat := range stats {
			expectedStat := expectedStats[index]

			require.Equal(t, expectedStat.Path, fetchedStat.Path)
			require.Equal(t, expectedStat.Additions, fetchedStat.Additions)
			require.Equal(t, expectedStat.Deletions, fetchedStat.Deletions)
		}
	}
}

func TestFailedDiffStatsRequest(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		desc          string
		repo          *gitalypb.Repository
		leftCommitID  string
		rightCommitID string
		err           codes.Code
	}{
		{
			desc:          "repo not found",
			repo:          &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar.git"},
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			err:           codes.NotFound,
		},
		{
			desc:          "storage not found",
			repo:          &gitalypb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			err:           codes.InvalidArgument,
		},
		{
			desc:          "left commit ID not found",
			repo:          testRepo,
			leftCommitID:  "",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			err:           codes.InvalidArgument,
		},
		{
			desc:          "right commit ID not found",
			repo:          testRepo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "",
			err:           codes.InvalidArgument,
		},
		{
			desc:          "invalid left commit",
			repo:          testRepo,
			leftCommitID:  "invalidinvalidinvalid",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			err:           codes.Unavailable,
		},
		{
			desc:          "invalid right commit",
			repo:          testRepo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "invalidinvalidinvalid",
			err:           codes.Unavailable,
		},
		{
			desc:          "left commit not found",
			repo:          testRepo,
			leftCommitID:  "z4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			err:           codes.Unavailable,
		},
		{
			desc:          "right commit not found",
			repo:          testRepo,
			leftCommitID:  "e4003da16c1c2c3fc4567700121b17bf8e591c6c",
			rightCommitID: "z4003da16c1c2c3fc4567700121b17bf8e591c6c",
			err:           codes.Unavailable,
		},
	}

	for _, tc := range tests {
		rpcRequest := &gitalypb.DiffStatsRequest{Repository: tc.repo, RightCommitId: tc.rightCommitID, LeftCommitId: tc.leftCommitID}
		stream, err := client.DiffStats(ctx, rpcRequest)
		require.NoError(t, err)

		t.Run(tc.desc, func(t *testing.T) {
			_, err := stream.Recv()

			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}
