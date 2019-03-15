package repository

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// getForkDestination creates a repo struct and path, but does not actually create the directory
func getForkDestination(t *testing.T) (*gitalypb.Repository, string, func()) {
	folder := fmt.Sprintf("%s_%s", t.Name(), strconv.Itoa(rand.New(rand.NewSource(time.Now().Unix())).Int()))
	forkRepoPath := filepath.Join(testhelper.GitlabTestStoragePath(), folder)
	forkedRepo := &gitalypb.Repository{StorageName: "default", RelativePath: folder, GlRepository: "project-1"}

	return forkedRepo, forkRepoPath, func() { os.RemoveAll(forkRepoPath) }
}

// getGitObjectDirSize gets the number of 1k blocks of a git object directory
func getGitObjectDirSize(t *testing.T, repoPath string) int64 {
	output := testhelper.MustRunCommand(t, nil, "du", "-s", "-k", filepath.Join(repoPath, "objects"))
	if len(output) < 2 {
		t.Error("invalid output of du -s -k")
	}

	outputSplit := strings.SplitN(string(output), "\t", 2)
	blocks, err := strconv.ParseInt(outputSplit[0], 10, 64)
	require.NoError(t, err)

	return blocks
}

func TestPreFetch(t *testing.T) {
	t.Skip("PreFetch is unsafe https://gitlab.com/gitlab-org/gitaly/issues/1552")

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, poolRepo := objectpool.NewTestObjectPool(t)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "gc")

	forkedRepo, forkRepoPath, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	req := &gitalypb.PreFetchRequest{
		TargetRepository: forkedRepo,
		SourceRepository: testRepo,
		ObjectPool: &gitalypb.ObjectPool{
			Repository: poolRepo,
		},
	}

	_, err := client.PreFetch(ctx, req)
	require.NoError(t, err)

	assert.True(t, getGitObjectDirSize(t, forkRepoPath) < 40)

	// feature is a branch known to exist in the source repository. By looking it up in the target
	// we establish that the target has branches, even though (as we saw above) it has no objects.
	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", "feature")
}

func TestPreFetchValidationError(t *testing.T) {
	t.Skip("PreFetch is unsafe https://gitlab.com/gitlab-org/gitaly/issues/1552")

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, poolRepo := objectpool.NewTestObjectPool(t)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	forkedRepo, _, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	badPool, _, cleanupBadPool := testhelper.NewTestRepo(t)
	defer cleanupBadPool()

	badPool.RelativePath = "bad_path"

	testCases := []struct {
		description string
		sourceRepo  *gitalypb.Repository
		targetRepo  *gitalypb.Repository
		objectPool  *gitalypb.Repository
		code        codes.Code
	}{
		{
			description: "source repository nil",
			sourceRepo:  nil,
			targetRepo:  forkedRepo,
			objectPool:  poolRepo,
			code:        codes.InvalidArgument,
		},
		{
			description: "target repository nil",
			sourceRepo:  testRepo,
			targetRepo:  nil,
			objectPool:  poolRepo,
			code:        codes.InvalidArgument,
		},
		{
			description: "source/target repository have different storage",
			sourceRepo:  testRepo,
			targetRepo: &gitalypb.Repository{
				StorageName:  "specialstorage",
				RelativePath: forkedRepo.RelativePath,
				GlRepository: forkedRepo.GlRepository,
			},
			objectPool: poolRepo,
			code:       codes.InvalidArgument,
		},
		{
			description: "bad pool repository",
			sourceRepo:  testRepo,
			targetRepo:  forkedRepo,
			objectPool:  badPool,
			code:        codes.FailedPrecondition,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.PreFetch(ctx, &gitalypb.PreFetchRequest{
				TargetRepository: tc.targetRepo,
				SourceRepository: tc.sourceRepo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: tc.objectPool,
				},
			})
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestPreFetchDirectoryExists(t *testing.T) {
	t.Skip("PreFetch is unsafe https://gitlab.com/gitlab-org/gitaly/issues/1552")

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	forkedRepo, _, forkRepoCleanup := testhelper.InitBareRepo(t)
	defer forkRepoCleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.PreFetch(ctx, &gitalypb.PreFetchRequest{TargetRepository: forkedRepo, SourceRepository: testRepo})
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
}
