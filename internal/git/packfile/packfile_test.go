package packfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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
	tempDir := testhelper.TempDir(t)

	emptyRepo := filepath.Join(tempDir, "empty.git")
	testhelper.MustRunCommand(t, nil, "git", "init", "--bare", emptyRepo)

	populatedRepo := filepath.Join(tempDir, "populated")
	testhelper.MustRunCommand(t, nil, "git", "init", populatedRepo)
	for i := 0; i < 10; i++ {
		testhelper.MustRunCommand(t, nil, "git", "-C", populatedRepo, "commit",
			"--allow-empty", "--message", "commit message")
	}
	testhelper.MustRunCommand(t, nil, "git", "-C", populatedRepo, "repack", "-ad")

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
			out, err := List(filepath.Join(tc.path, "objects"))
			require.NoError(t, err)
			require.Len(t, out, tc.numPacks)
		})
	}
}
