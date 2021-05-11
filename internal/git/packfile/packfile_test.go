package packfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/packfile"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
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
