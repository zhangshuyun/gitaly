package repository_test

import (
	"context"
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	gitaly_x509 "gitlab.com/gitlab-org/gitaly/internal/x509"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulCreateForkRequest(t *testing.T) {
	locator := config.NewLocator(config.Config)

	createEmptyTarget := func(repoPath string) {
		require.NoError(t, os.MkdirAll(repoPath, 0755))
	}

	// If this method is run multiple times in a test, TLS connections
	// will fail for some reason.
	testPool, sslCleanup := injectCustomCATestCerts(t)
	defer sslCleanup()

	for _, tt := range []struct {
		name          string
		secure        bool
		withPool      bool
		local         bool
		beforeRequest func(repoPath string)
	}{
		{name: "secure", secure: true},
		{name: "insecure"},
		{name: "secure in local mode", secure: true, local: true},
		{name: "insecure in local mode", local: true},
		{name: "existing empty directory target", beforeRequest: createEmptyTarget},
		{name: "secure with pool", secure: true, withPool: true},
		{name: "insecure with pool", withPool: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				serverSocketPath string
				client           gitalypb.RepositoryServiceClient
				conn             *grpc.ClientConn
			)

			if tt.secure {
				var serverCleanup testhelper.Cleanup
				_, serverSocketPath, serverCleanup = runFullSecureServer(t, locator)
				defer serverCleanup()

				client, conn = repository.NewSecureRepoClient(t, serverSocketPath, testPool)
				defer conn.Close()
			} else {
				var clean func()
				serverSocketPath, clean = runFullServer(t, locator)
				defer clean()

				client, conn = repository.NewRepositoryClient(t, serverSocketPath)
				defer conn.Close()
			}

			ctxOuter, cancel := testhelper.Context()
			defer cancel()

			var pool *objectpool.ObjectPool
			if tt.withPool {
				var poolCleanup func()
				pool, poolCleanup = createPool(ctxOuter, t)
				defer poolCleanup()
			}

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			forkedRepo := &gitalypb.Repository{
				RelativePath: "forks/test-repo-fork.git",
				StorageName:  testRepo.StorageName,
			}

			forkedRepoPath, err := locator.GetPath(forkedRepo)
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(forkedRepoPath))

			if tt.beforeRequest != nil {
				tt.beforeRequest(forkedRepoPath)
			}

			req := &gitalypb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: testRepo,
				AllowLocal:       tt.local,
			}

			if tt.withPool {
				req.Pool = pool.ToProto()
			}

			_, err = client.CreateFork(ctx, req)
			require.NoError(t, err)
			defer func() { require.NoError(t, os.RemoveAll(forkedRepoPath)) }()

			testhelper.MustRunCommand(t, nil, "git", "-C", forkedRepoPath, "fsck")

			remotes := testhelper.MustRunCommand(t, nil, "git", "-C", forkedRepoPath, "remote")
			require.NotContains(t, string(remotes), "origin")

			info, err := os.Lstat(filepath.Join(forkedRepoPath, "hooks"))
			require.NoError(t, err)
			require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)

			if tt.withPool {
				checkAlternatesFile(t, pool, locator, forkedRepo)
			}
		})
	}
}

func TestFailedCreateForkRequestDueToExistingTarget(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t, locator)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		repoPath string
		isDir    bool
	}{
		{
			desc:     "target is a non-empty directory",
			repoPath: "forks/test-repo-fork-dir.git",
			isDir:    true,
		},
		{
			desc:     "target is a file",
			repoPath: "forks/test-repo-fork-file.git",
			isDir:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			forkedRepo := &gitalypb.Repository{
				RelativePath: testCase.repoPath,
				StorageName:  testRepo.StorageName,
			}

			forkedRepoPath, err := locator.GetPath(forkedRepo)
			require.NoError(t, err)

			if testCase.isDir {
				require.NoError(t, os.MkdirAll(forkedRepoPath, 0770))
				require.NoError(t, ioutil.WriteFile(
					filepath.Join(forkedRepoPath, "config"),
					nil,
					0644,
				))
			} else {
				require.NoError(t, ioutil.WriteFile(forkedRepoPath, nil, 0644))
			}
			defer os.RemoveAll(forkedRepoPath)

			req := &gitalypb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: testRepo,
			}

			_, err = client.CreateFork(ctx, req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestCreateForkRequest_invalidPool(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t, locator)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, poolCleanup := createPool(ctx, t)
	defer poolCleanup()

	testCases := []struct {
		desc        string
		storage     string
		poolStorage string
		poolPath    string
		errMessage  string
	}{
		{
			desc:        "unknown storage",
			storage:     "unknown",
			poolStorage: "unknown",
			errMessage:  `GetStorageByName: no such storage: "unknown"`,
		},
		{
			desc:        "mismatched pool storages",
			storage:     testRepo.StorageName,
			poolStorage: "unknown",
			errMessage:  "target repository is on a different storage than the object pool",
		},
		{
			desc:        "unknown pool path",
			storage:     testRepo.StorageName,
			poolStorage: testRepo.StorageName,
			poolPath:    "/path/to/nowhere",
			errMessage:  "CreateFork: get object pool from request: invalid object pool directory",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			forkedRepo := &gitalypb.Repository{
				RelativePath: "forks/test-repo-fork.git",
				StorageName:  testCase.storage,
			}
			defer os.RemoveAll(filepath.Join(testhelper.GitlabTestStoragePath(), forkedRepo.RelativePath))

			poolProto := pool.ToProto()
			poolProto.Repository.StorageName = testCase.poolStorage

			if testCase.poolPath != "" {
				poolProto.Repository.RelativePath = testCase.poolPath
			}

			req := &gitalypb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: testRepo,
				Pool:             poolProto,
			}

			_, err := client.CreateFork(ctx, req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.True(t, testhelper.GrpcErrorHasMessage(err, testCase.errMessage))
		})
	}
}

func injectCustomCATestCerts(t *testing.T) (*x509.CertPool, testhelper.Cleanup) {
	certFile, keyFile, removeCerts := testhelper.GenerateTestCerts(t)

	oldTLSConfig := config.Config.TLS

	config.Config.TLS.CertPath = certFile
	config.Config.TLS.KeyPath = keyFile

	revertEnv := testhelper.ModifyEnvironment(t, gitaly_x509.SSLCertFile, certFile)
	cleanup := func() {
		config.Config.TLS = oldTLSConfig
		revertEnv()
		removeCerts()
	}

	caPEMBytes, err := ioutil.ReadFile(certFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEMBytes))

	return pool, cleanup
}

func createPool(ctx context.Context, t *testing.T) (*objectpool.ObjectPool, func()) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	relativePoolPath := testhelper.NewTestObjectPoolName(t)

	pool, err := objectpool.NewObjectPool(config.Config, config.NewLocator(config.Config), testRepo.GetStorageName(), relativePoolPath)
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	return pool, func() {
		cleanup()
		pool.Remove(ctx)
	}
}

func checkAlternatesFile(t *testing.T, pool *objectpool.ObjectPool, locator storage.Locator, testRepo *gitalypb.Repository) {
	altPath, err := locator.InfoAlternatesPath(testRepo)
	require.NoError(t, err)

	info, err := os.Lstat(altPath)
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)

	content, err := ioutil.ReadFile(altPath)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(content), "../"), "expected %q to be relative path", content)

	// Check that the forked repo has an alternate that points to the pool repository objects
	poolRepo := &gitalypb.Repository{
		RelativePath: pool.GetRelativePath(),
		StorageName:  pool.GetStorageName(),
	}
	poolPath, err := locator.GetRepoPath(poolRepo)
	require.NoError(t, err)
	expectedAlternatePath := filepath.Join(poolPath, "objects")

	repoPath, err := locator.GetRepoPath(testRepo)
	require.NoError(t, err)
	actualPath := filepath.Join(repoPath, "objects", string(content))
	require.Equal(t, expectedAlternatePath, actualPath)
}
