package localrepo

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRepo(t *testing.T) {
	git.TestRepository(t, func(t testing.TB, pbRepo *gitalypb.Repository) git.Repository {
		t.Helper()
		return New(pbRepo, config.Config)
	})
}
