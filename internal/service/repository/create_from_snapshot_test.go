package repository

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"google.golang.org/grpc/codes"

	"gitlab.com/gitlab-org/gitaly/internal/archive"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

var (
	secret       = "Magic secret"
	redirectPath = "/redirecting-snapshot.tar"
	tarPath      = "/snapshot.tar"
)

type testhandler struct {
	tarData io.Reader
}

func (h *testhandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != secret {
		http.Error(w, "Unuthorized", 401)
		return
	}

	switch r.RequestURI {
	case redirectPath:
		http.Redirect(w, r, tarPath, http.StatusFound)
	case tarPath:
		io.Copy(w, h.tarData)
	default:
		http.Error(w, "Not found", 404)
	}
}

// Create a tar file for the repo in memory, without relying on TarBuilder
func generateTarFile(t *testing.T, path string) ([]byte, []string) {
	data := testhelper.MustRunCommand(t, nil, "tar", "-C", path, "-cf", "-", ".")

	entries, err := archive.TarEntries(bytes.NewReader(data))
	require.NoError(t, err)

	return data, entries
}

func createFromSnapshot(t *testing.T, req *pb.CreateRepositoryFromSnapshotRequest) (*pb.CreateRepositoryFromSnapshotResponse, error) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	return client.CreateRepositoryFromSnapshot(ctx, req)
}

func TestCreateRepositoryFromSnapshotSuccess(t *testing.T) {
	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Ensure these won't be in the archive
	require.NoError(t, os.Remove(filepath.Join(repoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "hooks")))

	data, entries := generateTarFile(t, repoPath)

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&testhandler{bytes.NewReader(data)})
	defer srv.Close()

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoPath))

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, req)

	require.NoError(t, err)
	require.Equal(t, rsp, &pb.CreateRepositoryFromSnapshotResponse{})

	require.DirExists(t, repoPath)
	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			require.DirExists(t, filepath.Join(repoPath, entry), "directory %q not unpacked", entry)
		} else {
			require.FileExists(t, filepath.Join(repoPath, entry), "file %q not unpacked", entry)
		}
	}

	// hooks/ and config were excluded, but the RPC should create them
	require.FileExists(t, filepath.Join(repoPath, "config"), "Config file not created")

	fi, err := os.Lstat(filepath.Join(repoPath, "hooks"))
	require.NoError(t, err)
	require.Equal(t, os.ModeSymlink, fi.Mode()&os.ModeSymlink, "Symlink to global hooks not created")
}

func TestCreateRepositoryFromSnapshotFailsIfRepositoryExists(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	req := &pb.CreateRepositoryFromSnapshotRequest{Repository: testRepo}
	rsp, err := createFromSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotFailsIfBadURL(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	cleanupFn() // free up the destination dir for use

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    "invalid!scheme://invalid.invalid",
	}

	rsp, err := createFromSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotFailsIfBadAuth(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	cleanupFn() // free up the destination dir for use

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&testhandler{})
	defer srv.Close()

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   "Bad authentication",
	}

	rsp, err := createFromSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotFailsIfHttp404(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	cleanupFn() // free up the destination dir for use

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&testhandler{})
	defer srv.Close()

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    srv.URL + tarPath + ".does-not-exist",
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotDoesNotFollowRedirects(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	cleanupFn() // free up the destination dir for use

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&testhandler{})
	defer srv.Close()

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    srv.URL + redirectPath,
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotHandlesMalformedResponse(t *testing.T) {
	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	require.NoError(t, os.Remove(filepath.Join(repoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "hooks")))

	data, _ := generateTarFile(t, repoPath)
	// Only serve half of the tar file
	dataReader := io.LimitReader(bytes.NewReader(data), int64(len(data)/2))

	srv := httptest.NewServer(&testhandler{dataReader})
	defer srv.Close()

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoPath))

	req := &pb.CreateRepositoryFromSnapshotRequest{
		Repository: testRepo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, req)

	require.Error(t, err)
	require.Nil(t, rsp)

	// Ensure that a partial result is not left in place
	testhelper.AssertFileNotExists(t, repoPath)
}
