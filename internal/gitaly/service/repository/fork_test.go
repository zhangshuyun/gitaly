package repository_test

import (
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
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

	for _, tt := range []struct {
		name          string
		secure        bool
		beforeRequest func(repoPath string)
	}{
		{name: "secure", secure: true},
		{name: "insecure"},
		{name: "existing empty directory target", beforeRequest: createEmptyTarget},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				serverSocketPath string
				client           gitalypb.RepositoryServiceClient
				conn             *grpc.ClientConn
			)

			if tt.secure {
				testPool, sslCleanup := injectCustomCATestCerts(t)
				defer sslCleanup()

				var serverCleanup testhelper.Cleanup
				_, serverSocketPath, serverCleanup = runFullSecureServer(t, locator)
				defer serverCleanup()

				client, conn = repository.NewSecureRepoClient(t, serverSocketPath, testPool)
				defer conn.Close()
			} else {
				var clean func()
				serverSocketPath, clean = runFullServer(t)
				defer clean()

				client, conn = repository.NewRepositoryClient(t, serverSocketPath)
				defer conn.Close()
			}

			ctxOuter, cancel := testhelper.Context()
			defer cancel()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			testRepo, _, cleanupFn := gittest.CloneRepo(t)
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
		})
	}
}

func TestFailedCreateForkRequestDueToExistingTarget(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
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
