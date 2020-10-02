package packfile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}

func TestList(t *testing.T) {
	repoPath0 := "testdata/empty.git"
	require.NoError(t, os.RemoveAll(repoPath0))
	testhelper.MustRunCommand(t, nil, "git", "init", "--bare", repoPath0)

	_, repoPath1, cleanup1 := testhelper.NewTestRepo(t)
	defer cleanup1()
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath1, "repack", "-ad")

	testCases := []struct {
		desc     string
		path     string
		numPacks int
	}{
		{desc: "empty", path: repoPath0},
		{desc: "1 pack no alternates", path: repoPath1, numPacks: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			out, err := List(filepath.Join(tc.path, "objects"))
			require.NoError(t, err)
			require.Len(t, out, tc.numPacks)
		})
	}
}
