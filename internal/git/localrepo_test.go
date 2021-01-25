package git

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestLocalRepository(t *testing.T) {
	TestRepository(t, func(t testing.TB, pbRepo *gitalypb.Repository) Repository {
		t.Helper()
		return NewRepository(pbRepo, config.Config)
	})
}
