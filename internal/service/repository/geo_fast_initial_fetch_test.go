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

func TestGeoFastInitialFetch(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, remoteRepoPath, cleanupRemoteRepo := testhelper.NewTestRepo(t)
	defer cleanupRemoteRepo()

	// add a branch
	branch := "my-cool-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "update-ref", "refs/heads/"+branch, "master")

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, poolRepo := objectpool.NewTestObjectPool(t)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))

	forkedRepo, forkRepoPath, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	req := &gitalypb.GeoFastInitialFetchRequest{
		Repository: forkedRepo,
		ObjectPool: &gitalypb.ObjectPool{
			Repository: poolRepo,
		},
		Remote: &gitalypb.Remote{
			Url:                     remoteRepoPath,
			Name:                    "geo",
			HttpAuthorizationHeader: "token",
		},
	}

	_, err := client.GeoFastInitialFetch(ctx, req)
	require.NoError(t, err)

	assert.True(t, getGitObjectDirSize(t, forkRepoPath) < 40)

	// feature is a branch known to exist in the source repository. By looking it up in the target
	// we establish that the target has branches, even though (as we saw above) it has no objects.
	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", "feature")
	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", branch)
}

func TestGeoFastInitialFetchValidationError(t *testing.T) {
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
	require.NoError(t, pool.Link(ctx, testRepo, false))

	forkedRepo, _, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	badPool, _, cleanupBadPool := testhelper.NewTestRepo(t)
	defer cleanupBadPool()

	badPool.RelativePath = "bad_path"

	testCases := []struct {
		description string
		repo        *gitalypb.Repository
		objectPool  *gitalypb.Repository
		remoteURL   string
		code        codes.Code
	}{
		{
			description: "source repository nil",
			repo:        forkedRepo,
			objectPool:  poolRepo,
			remoteURL:   "something",
			code:        codes.InvalidArgument,
		},
		{
			description: "target repository nil",
			repo:        nil,
			objectPool:  poolRepo,
			remoteURL:   "something",
			code:        codes.InvalidArgument,
		},
		{
			description: "bad pool repository",
			repo:        forkedRepo,
			objectPool:  badPool,
			remoteURL:   "something",
			code:        codes.FailedPrecondition,
		},
		{
			description: "remote url is empty",
			repo:        forkedRepo,
			objectPool:  poolRepo,
			remoteURL:   "",
			code:        codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.GeoFastInitialFetch(ctx, &gitalypb.GeoFastInitialFetchRequest{
				Repository: tc.repo,
				ObjectPool: &gitalypb.ObjectPool{
					Repository: tc.objectPool,
				},
				Remote: &gitalypb.Remote{
					Name:                    "geo",
					Url:                     tc.remoteURL,
					HttpAuthorizationHeader: "header",
				},
			})
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestGeoFastInitialFetchDirectoryExists(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	forkedRepo, _, forkRepoCleanup := testhelper.InitBareRepo(t)
	defer forkRepoCleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.GeoFastInitialFetch(ctx, &gitalypb.GeoFastInitialFetchRequest{Repository: forkedRepo, ObjectPool: &gitalypb.ObjectPool{}})
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
}
