package stats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestLogObjectInfo(t *testing.T) {
	cfg := testcfg.Build(t)

	repo1, repoPath1 := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	repo2, repoPath2 := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	ctx := testhelper.Context(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	requireLog := func(entries []*logrus.Entry) map[string]interface{} {
		for _, entry := range entries {
			if entry.Message == "git repo statistic" {
				const key = "count_objects"
				data := entry.Data[key]
				require.NotNil(t, data, "there is no any information about statistics")
				countObjects, ok := data.(map[string]interface{})
				require.True(t, ok)
				require.Contains(t, countObjects, "count")
				require.Contains(t, countObjects, "size")
				require.Contains(t, countObjects, "in-pack")
				require.Contains(t, countObjects, "packs")
				require.Contains(t, countObjects, "size-pack")
				require.Contains(t, countObjects, "garbage")
				require.Contains(t, countObjects, "size-garbage")
				return countObjects
			}
		}
		return nil
	}

	t.Run("shared repo with multiple alternates", func(t *testing.T) {
		locator := config.NewLocator(cfg)
		storagePath, err := locator.GetStorageByName(repo1.GetStorageName())
		require.NoError(t, err)

		tmpDir, err := os.MkdirTemp(storagePath, "")
		require.NoError(t, err)
		defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

		// clone existing local repo with two alternates
		gittest.Exec(t, cfg, "clone", "--shared", repoPath1, "--reference", repoPath1, "--reference", repoPath2, tmpDir)

		logger, hook := test.NewNullLogger()
		testCtx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		LogObjectsInfo(testCtx, gitCmdFactory, &gitalypb.Repository{
			StorageName:  repo1.StorageName,
			RelativePath: filepath.Join(strings.TrimPrefix(tmpDir, storagePath), ".git"),
		})

		countObjects := requireLog(hook.AllEntries())
		require.ElementsMatch(t, []string{repoPath1 + "/objects", repoPath2 + "/objects"}, countObjects["alternate"])
	})

	t.Run("repo without alternates", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		testCtx := ctxlogrus.ToContext(ctx, logger.WithField("test", "logging"))

		LogObjectsInfo(testCtx, gitCmdFactory, repo2)

		countObjects := requireLog(hook.AllEntries())
		require.Contains(t, countObjects, "prune-packable")
	})
}
