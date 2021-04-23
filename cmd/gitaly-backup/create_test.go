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

func TestCreateSubcommand(t *testing.T) {
	cfg := testcfg.Build(t)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	var repos []*gitalypb.Repository
	for i := 0; i < 5; i++ {
		repo, _, _ := gittest.CloneRepoAtStorage(t, cfg.Storages[0], fmt.Sprintf("repo-%d", i))
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

	cmd := createSubcommand{backupPath: path}

	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	cmd.Flags(fs)

	require.NoError(t, fs.Parse([]string{"-path", path}))
	require.EqualError(t,
		cmd.Run(context.Background(), &stdin, ioutil.Discard),
		"create: 1 failures encountered")

	for _, repo := range repos {
		bundlePath := filepath.Join(path, repo.RelativePath+".bundle")
		require.FileExists(t, bundlePath)
	}
}
