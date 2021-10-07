package packfile_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/packfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestList(t *testing.T) {
	cfg := testcfg.Build(t)
	tempDir := testhelper.TempDir(t)

	emptyRepo := filepath.Join(tempDir, "empty.git")
	gittest.Exec(t, cfg, "init", "--bare", emptyRepo)

	populatedRepo := filepath.Join(tempDir, "populated")
	gittest.Exec(t, cfg, "init", populatedRepo)
	for i := 0; i < 10; i++ {
		gittest.Exec(t, cfg, "-C", populatedRepo, "commit",
			"--allow-empty", "--message", "commit message")
	}
	gittest.Exec(t, cfg, "-C", populatedRepo, "repack", "-ad")

	testCases := []struct {
		desc     string
		path     string
		numPacks int
	}{
		{desc: "empty", path: emptyRepo},
		{desc: "1 pack no alternates", path: filepath.Join(populatedRepo, ".git"), numPacks: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			out, err := packfile.List(filepath.Join(tc.path, "objects"))
			require.NoError(t, err)
			require.Len(t, out, tc.numPacks)
		})
	}
}
