package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRestoreSubcommand(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(
		featureflag.TxAtomicRepositoryCreation,
	).Run(t, testRestoreSubcommand)
}

func testRestoreSubcommand(t *testing.T, ctx context.Context) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	conn, err := client.Dial(gitalyAddr, nil)
	require.NoError(t, err)
	defer testhelper.MustClose(t, conn)

	existingRepo := &gitalypb.Repository{
		RelativePath: "existing_repo",
		StorageName:  cfg.Storages[0].Name,
	}

	// We need to create the repository entry in the database such that we can call
	// `RemoveRepository()` and have Praefect forward the request to Gitaly as expected.
	repoClient := gitalypb.NewRepositoryServiceClient(conn)
	_, err = repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: existingRepo})
	require.NoError(t, err)
	existingRepo.RelativePath = testhelper.GetReplicaPath(ctx, t, conn, existingRepo)

	// This is a bit awkward: we have to delete the created repository on-disk again and
	// recreate it with the seed repository.
	existRepoPath := filepath.Join(cfg.Storages[0].Path, existingRepo.RelativePath)
	require.NoError(t, os.RemoveAll(existRepoPath))
	gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: existingRepo.RelativePath,
	})
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
		"address":       "invalid",
		"token":         "invalid",
		"relative_path": "invalid",
	}))

	cmd := restoreSubcommand{}

	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	cmd.Flags(fs)

	require.NoError(t, fs.Parse([]string{"-path", path}))
	require.EqualError(t,
		cmd.Run(ctx, &stdin, io.Discard),
		"restore: pipeline: 1 failures encountered:\n - invalid: manager: remove repository: could not dial source: invalid connection string: \"invalid\"\n")

	for _, repo := range repos {
		repoPath := filepath.Join(cfg.Storages[0].Path, repo.RelativePath)
		bundlePath := filepath.Join(path, repo.RelativePath+".bundle")

		output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
		require.Contains(t, string(output), "The bundle records a complete history")
	}
}
