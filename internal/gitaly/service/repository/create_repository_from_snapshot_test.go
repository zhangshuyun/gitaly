package repository

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/praefectutil"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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
		if _, err := io.Copy(w, h.tarData); err != nil {
			panic(err)
		}
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

func createFromSnapshot(t *testing.T, ctx context.Context, req *gitalypb.CreateRepositoryFromSnapshotRequest, cfg config.Cfg) (*gitalypb.CreateRepositoryFromSnapshotResponse, error) {
	t.Helper()

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil)
	client := newRepositoryClient(t, cfg, serverSocketPath)

	return client.CreateRepositoryFromSnapshot(ctx, req)
}

func TestCreateRepositoryFromSnapshot_success(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromSnapshotSuccess)
}

func testCreateRepositoryFromSnapshotSuccess(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	// Ensure these won't be in the archive
	require.NoError(t, os.Remove(filepath.Join(sourceRepoPath, "config")))
	require.NoError(t, os.RemoveAll(filepath.Join(sourceRepoPath, "hooks")))

	data, entries := generateTarFile(t, sourceRepoPath)

	// Create a HTTP server that serves a given tar file
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewReader(data), secret: secret})
	defer srv.Close()

	repoRelativePath := filepath.Join("non-existing-parent", "repository")

	repo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: repoRelativePath,
	}
	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil)
	client := newRepositoryClient(t, cfg, serverSocketPath)

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, req)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})

	repoAbsolutePath := filepath.Join(cfg.Storages[0].Path, getReplicaPath(ctx, t, client, repo))
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

func TestCreateRepositoryFromSnapshot_repositoryExists(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromSnapshotFailsIfRepositoryExists)
}

func testCreateRepositoryFromSnapshotFailsIfRepositoryExists(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)

	// This creates the first repository on the server. As this test can run with Praefect in front of it,
	// we'll use the next replica path Praefect will assign in order to ensure this repository creation
	// conflicts even with Praefect in front of it.
	repo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: praefectutil.DeriveReplicaPath(1),
	})

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{Repository: repo}
	rsp, err := createFromSnapshot(t, ctx, req, cfg)

	if featureflag.TxAtomicRepositoryCreation.IsEnabled(ctx) {
		testhelper.RequireGrpcError(t, err, codes.AlreadyExists)
		require.Contains(t, err.Error(), "creating repository: repository exists already")
	} else {
		testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		require.Contains(t, err.Error(), "destination directory exists")
	}

	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshot_badURL(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromSnapshotFailsIfBadURL)
}

func testCreateRepositoryFromSnapshotFailsIfBadURL(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	require.NoError(t, os.RemoveAll(repoPath))

	req := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repo,
		HttpUrl:    "invalid!scheme://invalid.invalid",
	}

	rsp, err := createFromSnapshot(t, ctx, req, cfg)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "Bad HTTP URL")
	require.Nil(t, rsp)
}

func TestCreateRepositoryFromSnapshot_invalidArguments(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromSnapshotBadRequests)
}

func testCreateRepositoryFromSnapshotBadRequests(t *testing.T, ctx context.Context) {
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
			cfg := testcfg.Build(t)
			repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
			require.NoError(t, os.RemoveAll(repoPath))

			req := &gitalypb.CreateRepositoryFromSnapshotRequest{
				Repository: repo,
				HttpUrl:    srv.URL + tc.url,
				HttpAuth:   tc.auth,
			}

			rsp, err := createFromSnapshot(t, ctx, req, cfg)
			testhelper.RequireGrpcError(t, err, tc.code)
			require.Nil(t, rsp)

			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestCreateRepositoryFromSnapshot_malformedResponse(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromSnapshotHandlesMalformedResponse)
}

func testCreateRepositoryFromSnapshotHandlesMalformedResponse(t *testing.T, ctx context.Context) {
	cfg := testcfg.Build(t)
	repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

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

	rsp, err := createFromSnapshot(t, ctx, req, cfg)

	require.Error(t, err)
	require.Nil(t, rsp)

	// Ensure that a partial result is not left in place
	require.NoFileExists(t, repoPath)
}
