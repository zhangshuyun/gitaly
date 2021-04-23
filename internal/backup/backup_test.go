package backup

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestFilesystem_BackupRepository(t *testing.T) {
	cfg := testcfg.Build(t)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	hooksRepo, hooksRepoPath, _ := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "hooks")
	require.NoError(t, os.Mkdir(filepath.Join(hooksRepoPath, "custom_hooks"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(hooksRepoPath, "custom_hooks/pre-commit.sample"), []byte("Some hooks"), os.ModePerm))

	noHooksRepo, _, _ := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "no-hooks")
	emptyRepo, _, _ := gittest.InitBareRepoAt(t, cfg.Storages[0])
	nonexistentRepo := *emptyRepo
	nonexistentRepo.RelativePath = "nonexistent"

	for _, tc := range []struct {
		desc               string
		repo               *gitalypb.Repository
		createsBundle      bool
		createsCustomHooks bool
		err                error
	}{
		{
			desc:               "no hooks",
			repo:               noHooksRepo,
			createsBundle:      true,
			createsCustomHooks: false,
		},
		{
			desc:               "hooks",
			repo:               hooksRepo,
			createsBundle:      true,
			createsCustomHooks: true,
		},
		{
			desc:               "empty repo",
			repo:               emptyRepo,
			createsBundle:      false,
			createsCustomHooks: false,
			err:                ErrSkipped,
		},
		{
			desc:               "nonexistent repo",
			repo:               &nonexistentRepo,
			createsBundle:      false,
			createsCustomHooks: false,
			err:                ErrSkipped,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoPath := filepath.Join(cfg.Storages[0].Path, tc.repo.RelativePath)
			bundlePath := filepath.Join(path, tc.repo.RelativePath+".bundle")
			customHooksPath := filepath.Join(path, tc.repo.RelativePath, "custom_hooks.tar")

			ctx, cancel := testhelper.Context()
			defer cancel()

			fsBackup := NewFilesystem(path)
			err := fsBackup.BackupRepository(ctx, storage.ServerInfo{Address: gitalyAddr, Token: cfg.Auth.Token}, tc.repo)
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err, err)
			}

			if tc.createsBundle {
				require.FileExists(t, bundlePath)

				output := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "bundle", "verify", bundlePath)
				require.Contains(t, string(output), "The bundle records a complete history")
			} else {
				require.NoFileExists(t, bundlePath)
			}

			if tc.createsCustomHooks {
				require.FileExists(t, customHooksPath)
			} else {
				require.NoFileExists(t, customHooksPath)
			}
		})
	}
}
