package localrepo

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRepo(t *testing.T) {
	cfg, cleanup := testcfg.Build(t)
	defer cleanup()

	gittest.TestRepository(t, cfg, func(t testing.TB, pbRepo *gitalypb.Repository) git.Repository {
		t.Helper()
		return New(git.NewExecCommandFactory(cfg), pbRepo, cfg)
	})
}
