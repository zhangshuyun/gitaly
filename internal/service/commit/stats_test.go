package commit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestCommitStatsSuccess(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		desc                 string
		revision             string
		oid                  string
		additions, deletions int32
	}{
		{
			desc:      "multiple changes, multiple files",
			revision:  "test-do-not-touch",
			oid:       "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1",
			additions: 27,
			deletions: 59,
		},
		{
			desc:      "multiple changes, multiple files, reference by commit ID",
			revision:  "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1",
			oid:       "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1",
			additions: 27,
			deletions: 59,
		},
		{
			desc:      "merge commit",
			revision:  "60ecb67",
			oid:       "60ecb67744cb56576c30214ff52294f8ce2def98",
			additions: 1,
			deletions: 0,
		},
		{
			desc:      "binary file",
			revision:  "ae73cb0",
			oid:       "ae73cb07c9eeaf35924a10f713b364d32b2dd34f",
			additions: 0,
			deletions: 0,
		},
		{
			desc:      "initial commit",
			revision:  "1a0b36b3",
			oid:       "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			additions: 43,
			deletions: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.CommitStats(ctx, &gitalypb.CommitStatsRequest{
				Repository: testRepo,
				Revision:   []byte(tc.revision),
			})
			require.NoError(t, err)

			assert.Equal(t, tc.oid, resp.GetOid())
			assert.Equal(t, tc.additions, resp.GetAdditions())
			assert.Equal(t, tc.deletions, resp.GetDeletions())
		})
	}
}

func TestCommitStatsFailure(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		desc     string
		repo     *gitalypb.Repository
		revision []byte
		err      codes.Code
	}{
		{
			desc:     "repo not found",
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar.git"},
			revision: []byte("test-do-not-touch"),
			err:      codes.NotFound,
		},
		{
			desc:     "storage not found",
			repo:     &gitalypb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			revision: []byte("test-do-not-touch"),
			err:      codes.InvalidArgument,
		},
		{
			desc:     "ref not found",
			repo:     testRepo,
			revision: []byte("non/existing"),
			err:      codes.Internal,
		},
		{
			desc:     "invalid revision",
			repo:     testRepo,
			revision: []byte("--outpu=/meow"),
			err:      codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CommitStats(ctx, &gitalypb.CommitStatsRequest{Repository: tc.repo, Revision: tc.revision})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}
