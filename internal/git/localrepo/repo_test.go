package localrepo

import (
	"context"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRepo(t *testing.T) {
	cfg := testcfg.Build(t)

	gittest.TestRepository(t, cfg, func(ctx context.Context, t testing.TB, seeded bool) (git.Repository, string) {
		t.Helper()

		var (
			pbRepo   *gitalypb.Repository
			repoPath string
		)

		if seeded {
			pbRepo, repoPath = gittest.CloneRepo(t, cfg, cfg.Storages[0])
		} else {
			pbRepo, repoPath = gittest.InitRepo(t, cfg, cfg.Storages[0])
		}

		gitCmdFactory := gittest.NewCommandFactory(t, cfg)
		catfileCache := catfile.NewCache(cfg)
		t.Cleanup(catfileCache.Stop)
		return New(config.NewLocator(cfg), gitCmdFactory, catfileCache, pbRepo), repoPath
	})
}
