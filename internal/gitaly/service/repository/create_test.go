package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepoNoAuth(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "some"}}))

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil)
	client := newRepositoryClient(t, config.Cfg{Auth: auth.Config{Token: ""}}, serverSocketPath)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func TestCreateRepositorySuccess(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	storageDir := cfg.Storages[0].Path
	relativePath := "create-repository-test.git"
	repoDir := filepath.Join(storageDir, relativePath)
	require.NoError(t, os.RemoveAll(repoDir))

	repo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: relativePath}
	req := &gitalypb.CreateRepositoryRequest{Repository: repo}
	_, err := client.CreateRepository(ctx, req)
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(repoDir)) }()

	fi, err := os.Stat(repoDir)
	require.NoError(t, err)
	require.Equal(t, "drwxr-x---", fi.Mode().String())

	for _, dir := range []string{repoDir, filepath.Join(repoDir, "refs")} {
		fi, err := os.Stat(dir)
		require.NoError(t, err)
		require.True(t, fi.IsDir(), "%q must be a directory", fi.Name())
	}

	symRef, err := ioutil.ReadFile(path.Join(repoDir, "HEAD"))
	require.NoError(t, err)

	require.Equal(t, symRef, []byte(fmt.Sprintf("ref: %s\n", git.DefaultRef)))
}

func TestCreateRepositoryFailure(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	storagePath := cfg.Storages[0].Path
	fullPath := filepath.Join(storagePath, "foo.git")

	_, err := os.Create(fullPath)
	require.NoError(t, err)
	defer os.RemoveAll(fullPath)

	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "foo.git"},
	})

	testhelper.RequireGrpcError(t, err, codes.Internal)
}

func TestCreateRepositoryFailureInvalidArgs(t *testing.T) {
	_, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		repo *gitalypb.Repository
		code codes.Code
	}{
		{
			repo: &gitalypb.Repository{StorageName: "does not exist", RelativePath: "foobar.git"},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc.repo), func(t *testing.T) {
			_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: tc.repo})

			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestCreateRepositoryIdempotent(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	refsBefore := strings.Split(string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref")), "\n")

	req := &gitalypb.CreateRepositoryRequest{Repository: repo}
	_, err := client.CreateRepository(ctx, req)
	require.NoError(t, err)

	refsAfter := strings.Split(string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref")), "\n")

	assert.Equal(t, refsBefore, refsAfter)
}
