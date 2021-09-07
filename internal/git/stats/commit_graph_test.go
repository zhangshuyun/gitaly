package stats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestIsMissingBloomFilters(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		desc   string
		enable bool
	}{
		{desc: "no Bloom filter", enable: false},
		{desc: "with Bloom filter", enable: true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := testcfg.Build(t)
			_, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

			args := []string{"-C", repoPath, "commit-graph", "write", "--reachable", "--split"}
			if tc.enable {
				args = append(args, "--changed-paths")
			}
			gittest.Exec(t, cfg, args...)

			ok, err := IsMissingBloomFilters(repoPath)
			require.NoError(t, err)
			require.Equal(t, tc.enable, !ok)
		})
	}
}
