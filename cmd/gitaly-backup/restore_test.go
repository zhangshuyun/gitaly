package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRestoreSubcommand(t *testing.T) {
	cfg := testcfg.Build(t)
	testhelper.ConfigureGitalyHooksBinary(cfg.BinDir)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	existingRepo, existRepoPath, _ := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "existing_repo")
	existingRepoBundlePath := filepath.Join(path, existingRepo.RelativePath+".bundle")
	gittest.Exec(t, cfg, "-C", existRepoPath, "bundle", "create", existingRepoBundlePath, "--all")

	repos := []*gitalypb.Repository{existingRepo}
	for i := 0; i < 2; i++ {
		repo := gittest.InitRepoDir(t, cfg.Storages[0].Path, fmt.Sprintf("repo-%d", i))
		repoBundlePath := filepath.Join(path, repo.RelativePath+".bundle")
		testhelper.CopyFile(t, existingRepoBundlePath, repoBundlePath)
		repos = append(repos, repo)
	}

	var stdin bytes.Buffer

	encoder := json.NewEncoder(&stdin)
	for _, repo := range repos {
		require.NoError(t, encoder.Encode(map[string]string{
			"address":         gitalyAddr,
			"token":           cfg.Auth.Token,
			"storage_name":    repo.StorageName,
			"relative_path":   repo.RelativePath,
			"gl_project_path": repo.GlProjectPath,
		}))
	}

	require.NoError(t, encoder.Encode(map[string]string{
		"address": "invalid",
		"token":   "invalid",
	}))

	cmd := restoreSubcommand{}

	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	cmd.Flags(fs)

	require.NoError(t, fs.Parse([]string{"-path", path}))
	require.EqualError(t,
		cmd.Run(context.Background(), &stdin, ioutil.Discard),
		"restore: 1 failures encountered")

	for _, repo := range repos {
		repoPath := filepath.Join(cfg.Storages[0].Path, repo.RelativePath)
		bundlePath := filepath.Join(path, repo.RelativePath+".bundle")

		output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
		require.Contains(t, string(output), "The bundle records a complete history")
	}
}
