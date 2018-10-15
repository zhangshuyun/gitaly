package commit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestCommitStatsSuccess(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		revision             []byte
		oid                  string
		additions, deletions int32
	}{
		{
			revision:  []byte("test-do-not-touch"),
			oid:       "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1",
			additions: 27,
			deletions: 59,
		},
		{
			revision:  []byte("899d3d27b04690ac1cd9ef4d8a74fde0667c57f1"),
			oid:       "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1",
			additions: 27,
			deletions: 59,
		},
	}

	for _, tc := range tests {
		resp, err := client.CommitStats(ctx, &gitalypb.CommitStatsRequest{Repository: testRepo, Revision: tc.revision})
		assert.NoError(t, err)
		assert.Equal(t, tc.oid, resp.GetOid())
		assert.Equal(t, tc.additions, resp.GetAdditions())
		assert.Equal(t, tc.deletions, resp.GetDeletions())
	}
}

func TestCommitStatsFailure(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

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
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.CommitStats(ctx, &gitalypb.CommitStatsRequest{Repository: tc.repo, Revision: tc.revision})
			testhelper.RequireGrpcError(t, err, tc.err)
		})
	}
}
