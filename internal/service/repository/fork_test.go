package repository_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	gitaly_x509 "gitlab.com/gitlab-org/gitaly/internal/x509"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
				server, serverSocketPath = runFullServer(t, config.Config.Storages)
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

			forkedRepoPath, err := helper.GetRepositoryPath(forkedRepo, config.Config.Storages)
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

			info, err := os.Lstat(path.Join(forkedRepoPath, "hooks"))
			require.NoError(t, err)
			require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)
		})
	}
}

func TestFailedCreateForkRequestDueToExistingTarget(t *testing.T) {
	server, serverSocketPath := runFullServer(t, config.Config.Storages)
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

			forkedRepoPath, err := helper.GetRepositoryPath(forkedRepo, config.Config.Storages)
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
	rootCA := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(0, 0, 1),
		IsCA:         true,
		DNSNames:     []string{"localhost"},
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caCert, err := x509.CreateCertificate(rand.Reader, rootCA, rootCA, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	entityKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	entityX509 := &x509.Certificate{
		SerialNumber: big.NewInt(2),
	}

	entityCert, err := x509.CreateCertificate(
		rand.Reader, rootCA, entityX509, &entityKey.PublicKey, caKey)
	require.NoError(t, err)

	certFile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer certFile.Close()

	caPEMBytes := &bytes.Buffer{}
	certPEMWriter := io.MultiWriter(certFile, caPEMBytes)

	// create chained PEM file with CA and entity cert
	for _, cert := range [][]byte{entityCert, caCert} {
		require.NoError(t,
			pem.Encode(certPEMWriter, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cert,
			}),
		)
	}

	keyFile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer keyFile.Close()

	entityKeyBytes, err := x509.MarshalECPrivateKey(entityKey)
	require.NoError(t, err)

	require.NoError(t,
		pem.Encode(keyFile, &pem.Block{
			Type:  "ECDSA PRIVATE KEY",
			Bytes: entityKeyBytes,
		}),
	)

	oldTLSConfig := config.Config.TLS

	config.Config.TLS.CertPath = certFile.Name()
	config.Config.TLS.KeyPath = keyFile.Name()

	oldSSLCert := os.Getenv(gitaly_x509.SSLCertFile)
	os.Setenv(gitaly_x509.SSLCertFile, certFile.Name())

	cleanup := func() {
		config.Config.TLS = oldTLSConfig
		os.Setenv(gitaly_x509.SSLCertFile, oldSSLCert)
		require.NoError(t, os.Remove(certFile.Name()))
		require.NoError(t, os.Remove(keyFile.Name()))
	}

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEMBytes.Bytes()))

	return pool, cleanup
}
