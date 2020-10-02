package repository_test

import (
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	gitaly_x509 "gitlab.com/gitlab-org/gitaly/v13/internal/x509"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulCreateForkRequest(t *testing.T) {
	for _, tt := range []struct {
		name   string
		secure bool
	}{
		{name: "secure", secure: true},
		{name: "insecure"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				server           *grpc.Server
				serverSocketPath string
				client           gitalypb.RepositoryServiceClient
				conn             *grpc.ClientConn
			)

			if tt.secure {
				testPool, sslCleanup := injectCustomCATestCerts(t)
				defer sslCleanup()

				var serverCleanup testhelper.Cleanup
				_, serverSocketPath, serverCleanup = runFullSecureServer(t)
				defer serverCleanup()

				client, conn = repository.NewSecureRepoClient(t, serverSocketPath, testPool)
				defer conn.Close()
			} else {
				server, serverSocketPath = runFullServer(t)
				defer server.Stop()

				client, conn = repository.NewRepositoryClient(t, serverSocketPath)
				defer conn.Close()
			}

			ctxOuter, cancel := testhelper.Context()
			defer cancel()

			md := testhelper.GitalyServersMetadata(t, serverSocketPath)
			ctx := metadata.NewOutgoingContext(ctxOuter, md)

			testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			forkedRepo := &gitalypb.Repository{
				RelativePath: "forks/test-repo-fork.git",
				StorageName:  testRepo.StorageName,
			}

			forkedRepoPath, err := helper.GetPath(forkedRepo)
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(forkedRepoPath))

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
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

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
			desc:     "target is a directory",
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

			forkedRepoPath, err := helper.GetPath(forkedRepo)
			require.NoError(t, err)

			if testCase.isDir {
				require.NoError(t, os.MkdirAll(forkedRepoPath, 0770))
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
