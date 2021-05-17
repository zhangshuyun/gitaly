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
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	secret       = "Magic secret"
	redirectPath = "/redirecting-snapshot.tar"
	tarPath      = "/snapshot.tar"
)

type tarTesthandler struct {
	tarData io.Reader
	secret  string
}

func (h *tarTesthandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != h.secret {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
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

func createFromSnapshot(t *testing.T, req *gitalypb.CreateRepositoryFromSnapshotRequest, cfg config.Cfg) (*gitalypb.CreateRepositoryFromSnapshotResponse, error) {
	t.Helper()

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil)
	client := newRepositoryClient(t, cfg, serverSocketPath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	return client.CreateRepositoryFromSnapshot(ctx, req)
}

func TestCreateRepositoryFromSnapshotSuccess(t *testing.T) {
	cfg := testcfg.Build(t)
	_, sourceRepoPath, cleanTestRepo := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanTestRepo)

	// Ensure these won't be in the archive
	require.NoError(t, os.Remove(filepath.Join(sourceRepoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(sourceRepoPath, "hooks")))

	data, entries := generateTarFile(t, sourceRepoPath)

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewReader(data), secret: secret})
	defer srv.Close()

	repoRelativePath := filepath.Join("non-existing-parent", "repository")

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: repoRelativePath,
		},
		HttpUrl:  srv.URL + tarPath,
		HttpAuth: secret,
	}

	rsp, err := createFromSnapshot(t, req, cfg)

	require.NoError(t, err)
	testhelper.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})

	repoAbsolutePath := filepath.Join(cfg.Storages[0].Path, repoRelativePath)
	require.DirExists(t, repoAbsolutePath)
	for _, entry := range entries {
		if strings.HasSuffix(entry, "/") {
			require.DirExists(t, filepath.Join(repoAbsolutePath, entry), "directory %q not unpacked", entry)
		} else {
			require.FileExists(t, filepath.Join(repoAbsolutePath, entry), "file %q not unpacked", entry)
		}
	}

	// hooks/ and config were excluded, but the RPC should create them
	require.FileExists(t, filepath.Join(repoAbsolutePath, "config"), "Config file not created")
}

func TestCreateRepositoryFromSnapshotFailsIfRepositoryExists(t *testing.T) {
	cfg := testcfg.Build(t)
	repo, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanupFn)

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{Repository: repo}
	rsp, err := createFromSnapshot(t, req, cfg)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "destination directory exists")
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotFailsIfBadURL(t *testing.T) {
	cfg := testcfg.Build(t)
	repo, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	cleanupFn() // free up the destination dir for use

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    "invalid!scheme://invalid.invalid",
	}

	rsp, err := createFromSnapshot(t, req, cfg)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "Bad HTTP URL")
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshotBadRequests(t *testing.T) {
	cfg := testcfg.Build(t)
	repo, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	cleanupFn() // free up the destination dir for use

	testCases := []struct {
		desc        string
		url         string
		auth        string
		code        codes.Code
		errContains string
	}{
		{
			desc:        "http bad auth",
			url:         tarPath,
			auth:        "Bad authentication",
			code:        codes.Internal,
			errContains: "HTTP server: 401 ",
		},
		{
			desc:        "http not found",
			url:         tarPath + ".does-not-exist",
			auth:        secret,
			code:        codes.Internal,
			errContains: "HTTP server: 404 ",
		},
		{
			desc:        "http do not follow redirects",
			url:         redirectPath,
			auth:        secret,
			code:        codes.Internal,
			errContains: "HTTP server: 302 ",
		},
	}

	srv := httptest.NewServer(&tarTesthandler{secret: secret})
	defer srv.Close()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			req := &gitalypb.CreateRepositoryFromSnapshotRequest{
				Repository: repo,
				HttpUrl:    srv.URL + tc.url,
				HttpAuth:   tc.auth,
			}

			rsp, err := createFromSnapshot(t, req, cfg)
			testhelper.RequireGrpcError(t, err, tc.code)
			require.Nil(t, rsp)

			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestCreateRepositoryFromSnapshotHandlesMalformedResponse(t *testing.T) {
	cfg := testcfg.Build(t)
	repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanupFn)

	require.NoError(t, os.Remove(filepath.Join(repoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "hooks")))

	data, _ := generateTarFile(t, repoPath)
	// Only serve half of the tar file
	dataReader := io.LimitReader(bytes.NewReader(data), int64(len(data)/2))

	srv := httptest.NewServer(&tarTesthandler{tarData: dataReader, secret: secret})
	defer srv.Close()

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoPath))

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, req, cfg)

	require.Error(t, err)
	require.Nil(t, rsp)

	// Ensure that a partial result is not left in place
	require.NoFileExists(t, repoPath)
}
