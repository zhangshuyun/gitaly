package config

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigLocator_GetObjectDirectoryPath(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	repoPath := filepath.Join(tmpDir, "relative")
	require.NoError(t, os.MkdirAll(repoPath, 0755))

	cfg := Cfg{
		Storages: []Storage{{
			Name: "gitaly-1",
			Path: filepath.Dir(repoPath),
		}},
	}
	require.NoError(t, cfg.SetGitPath())
	cmd := exec.Command(cfg.Git.BinPath, "init", "--bare", "--quiet")
	cmd.Dir = repoPath
	require.NoError(t, cmd.Run())

	locator := NewLocator(cfg)

	repoWithGitObjDir := func(dir string) *gitalypb.Repository {
		return &gitalypb.Repository{
			StorageName:        "gitaly-1",
			RelativePath:       filepath.Base(repoPath),
			GlRepository:       "gl_repo",
			GitObjectDirectory: dir,
		}
	}

	testRepo := repoWithGitObjDir("")

	testCases := []struct {
		desc string
		repo *gitalypb.Repository
		path string
		err  codes.Code
	}{
		{
			desc: "storages configured",
			repo: repoWithGitObjDir("objects/"),
			path: filepath.Join(repoPath, "objects/"),
		},
		{
			desc: "no GitObjectDirectoryPath",
			repo: testRepo,
			err:  codes.InvalidArgument,
		},
		{
			desc: "with directory traversal",
			repo: repoWithGitObjDir("../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "valid path but doesn't exist",
			repo: repoWithGitObjDir("foo../bazqux.git"),
			err:  codes.NotFound,
		},
		{
			desc: "with sneaky directory traversal",
			repo: repoWithGitObjDir("/../bazqux.git"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository",
			repo: repoWithGitObjDir("objects/../.."),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository with trailing separator",
			repo: repoWithGitObjDir("objects/../../"),
			err:  codes.InvalidArgument,
		},
		{
			desc: "with deep traversal at the end",
			repo: repoWithGitObjDir("bazqux.git/../.."),
			err:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			path, err := locator.GetObjectDirectoryPath(tc.repo)

			if tc.err != codes.OK {
				st, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, tc.err, st.Code())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.path, path)
		})
	}
}
