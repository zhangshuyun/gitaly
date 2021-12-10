package testhelper

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// RepositoryAuthToken is the default token used to authenticate
	// against other Gitaly servers. It is inject as part of the
	// GitalyServers metadata.
	RepositoryAuthToken = "the-secret-token"
	// DefaultStorageName is the default name of the Gitaly storage.
	DefaultStorageName = "default"
)

// GetReplicaPath retrieves the repository's replica path if the test has been
// run with Praefect in front of it. This is necessary if the test creates a repository
// through Praefect and peeks into the filesystem afterwards. Conn should be pointing to
// Praefect.
func GetReplicaPath(ctx context.Context, t testing.TB, conn *grpc.ClientConn, repo *gitalypb.Repository) string {
	metadata, err := gitalypb.NewPraefectInfoServiceClient(conn).GetRepositoryMetadata(
		ctx, &gitalypb.GetRepositoryMetadataRequest{
			Query: &gitalypb.GetRepositoryMetadataRequest_Path_{
				Path: &gitalypb.GetRepositoryMetadataRequest_Path{
					VirtualStorage: repo.GetStorageName(),
					RelativePath:   repo.GetRelativePath(),
				},
			},
		})
	if status, ok := status.FromError(err); ok && status.Code() == codes.Unimplemented && status.Message() == "unknown service gitaly.PraefectInfoService" {
		// The repository is stored at relative path if the test is running without Praefect in front.
		return repo.RelativePath
	}
	require.NoError(t, err)

	return metadata.ReplicaPath
}

// SkipWithPraefect skips the test if it is being executed with Praefect in front
// of the Gitaly.
func SkipWithPraefect(t testing.TB, reason string) {
	if os.Getenv("GITALY_TEST_WITH_PRAEFECT") == "YesPlease" {
		t.Skipf(reason)
	}
}

// MustReadFile returns the content of a file or fails at once.
func MustReadFile(t testing.TB, filename string) []byte {
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	return content
}

// GitlabTestStoragePath returns the storage path to the gitlab-test repo.
func GitlabTestStoragePath() string {
	if testDirectory == "" {
		panic("you must call testhelper.Configure() before GitlabTestStoragePath()")
	}
	return filepath.Join(testDirectory, "storage")
}

// MustRunCommand runs a command with an optional standard input and returns the standard output, or fails.
func MustRunCommand(t testing.TB, stdin io.Reader, name string, args ...string) []byte {
	t.Helper()

	if filepath.Base(name) == "git" {
		require.Fail(t, "Please use gittest.Exec or gittest.ExecStream to run git commands.")
	}

	cmd := exec.Command(name, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}

	output, err := cmd.Output()
	if err != nil {
		stderr := err.(*exec.ExitError).Stderr
		require.NoError(t, err, "%s %s: %s", name, args, stderr)
	}

	return output
}

// MustClose calls Close() on the Closer and fails the test in case it returns
// an error. This function is useful when closing via `defer`, as a simple
// `defer require.NoError(t, closer.Close())` would cause `closer.Close()` to
// be executed early already.
func MustClose(t testing.TB, closer io.Closer) {
	require.NoError(t, closer.Close())
}

// CopyFile copies a file at the path src to a file at the path dst
func CopyFile(t testing.TB, src, dst string) {
	fsrc, err := os.Open(src)
	require.NoError(t, err)
	defer MustClose(t, fsrc)

	fdst, err := os.Create(dst)
	require.NoError(t, err)
	defer MustClose(t, fdst)

	_, err = io.Copy(fdst, fsrc)
	require.NoError(t, err)
}

// GetTemporaryGitalySocketFileName will return a unique, useable socket file name
func GetTemporaryGitalySocketFileName(t testing.TB) string {
	require.NotEmpty(t, testDirectory, "you must call testhelper.Configure() before GetTemporaryGitalySocketFileName()")

	tmpfile, err := os.CreateTemp(testDirectory, "gitaly.socket.")
	require.NoError(t, err)

	name := tmpfile.Name()
	require.NoError(t, tmpfile.Close())
	require.NoError(t, os.Remove(name))

	return name
}

// GetLocalhostListener listens on the next available TCP port and returns
// the listener and the localhost address (host:port) string.
func GetLocalhostListener(t testing.TB) (net.Listener, string) {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	addr := fmt.Sprintf("localhost:%d", l.Addr().(*net.TCPAddr).Port)

	return l, addr
}

// ContextOpt returns a new context instance with the new additions to it.
type ContextOpt func(context.Context) context.Context

// ContextWithLogger allows to inject provided logger into the context.
func ContextWithLogger(logger *log.Entry) ContextOpt {
	return func(ctx context.Context) context.Context {
		return ctxlogrus.ToContext(ctx, logger)
	}
}

// Context returns a cancellable context.
func Context(opts ...ContextOpt) (context.Context, func()) {
	return context.WithCancel(ContextWithoutCancel(opts...))
}

// ContextWithoutCancel returns a non-cancellable context.
func ContextWithoutCancel(opts ...ContextOpt) context.Context {
	ctx := context.Background()

	// Enable use of explicit feature flags. Each feature flag which is checked must have been
	// explicitly injected into the context, or otherwise we panic. This is a sanity check to
	// verify that all feature flags we introduce are tested both with the flag enabled and
	// with the flag disabled.
	ctx = featureflag.ContextWithExplicitFeatureFlags(ctx)
	// There are some feature flags we need to enable in this function because they end up very
	// deep in the call stack, so almost every test function would have to inject it into its
	// context.
	ctx = featureflag.ContextWithFeatureFlags(ctx, featureflag.RunCommandsInCGroup)

	for _, opt := range opts {
		ctx = opt(ctx)
	}

	return ctx
}

// CreateGlobalDirectory creates a directory in the test directory that is shared across all
// between all tests.
func CreateGlobalDirectory(t testing.TB, name string) string {
	require.NotEmpty(t, testDirectory, "global temporary directory does not exist")
	path := filepath.Join(testDirectory, name)
	require.NoError(t, os.Mkdir(path, 0o777))
	return path
}

// TempDir is a wrapper around os.MkdirTemp that provides a cleanup function.
func TempDir(t testing.TB) string {
	if testDirectory == "" {
		panic("you must call testhelper.Configure() before TempDir()")
	}

	tmpDir, err := os.MkdirTemp(testDirectory, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	})

	return tmpDir
}

// Cleanup functions should be called in a defer statement
// immediately after they are returned from a test helper
type Cleanup func()

// WriteExecutable ensures that the parent directory exists, and writes an executable with provided
// content. The executable must not exist previous to writing it.
func WriteExecutable(t testing.TB, path string, content []byte) {
	dir := filepath.Dir(path)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	executable, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	require.NoError(t, err)
	defer MustClose(t, executable)

	_, err = io.Copy(executable, bytes.NewReader(content))
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(dir))
	})
}

// ModifyEnvironment will change an environment variable and return a func suitable
// for `defer` to change the value back. If the given value is empty, then the envvar will be
// unset.
func ModifyEnvironment(t testing.TB, key string, value string) func() {
	t.Helper()

	oldValue, hasOldValue := os.LookupEnv(key)
	if value == "" {
		require.NoError(t, os.Unsetenv(key))
	} else {
		require.NoError(t, os.Setenv(key, value))
	}

	return func() {
		if hasOldValue {
			require.NoError(t, os.Setenv(key, oldValue))
		} else {
			require.NoError(t, os.Unsetenv(key))
		}
	}
}

// GenerateCerts creates a certificate that can be used to establish TLS protected TCP connection.
// It returns paths to the file with the certificate and its private key.
func GenerateCerts(t *testing.T) (string, string) {
	t.Helper()

	rootCA := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(0, 0, 1),
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("0.0.0.0"), net.ParseIP("127.0.0.1"), net.ParseIP("::1"), net.ParseIP("::")},
		DNSNames:              []string{"localhost"},
		KeyUsage:              x509.KeyUsageCertSign,
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

	entityCert, err := x509.CreateCertificate(rand.Reader, rootCA, entityX509, &entityKey.PublicKey, caKey)
	require.NoError(t, err)

	certFile, err := os.CreateTemp(testDirectory, "")
	require.NoError(t, err)
	defer MustClose(t, certFile)
	t.Cleanup(func() {
		require.NoError(t, os.Remove(certFile.Name()))
	})

	// create chained PEM file with CA and entity cert
	for _, cert := range [][]byte{entityCert, caCert} {
		require.NoError(t,
			pem.Encode(certFile, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cert,
			}),
		)
	}

	keyFile, err := os.CreateTemp(testDirectory, "")
	require.NoError(t, err)
	defer MustClose(t, keyFile)
	t.Cleanup(func() {
		require.NoError(t, os.Remove(keyFile.Name()))
	})

	entityKeyBytes, err := x509.MarshalECPrivateKey(entityKey)
	require.NoError(t, err)

	require.NoError(t,
		pem.Encode(keyFile, &pem.Block{
			Type:  "ECDSA PRIVATE KEY",
			Bytes: entityKeyBytes,
		}),
	)

	return certFile.Name(), keyFile.Name()
}
