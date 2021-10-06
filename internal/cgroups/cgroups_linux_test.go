package cgroups

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestNewManager(t *testing.T) {
	cfg := cgroups.Config{Count: 10}

	require.IsType(t, &CGroupV1Manager{}, &CGroupV1Manager{cfg: cfg})
	require.IsType(t, &NoopManager{}, NewManager(cgroups.Config{}))
}
