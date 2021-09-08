package catfile

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestObjectInfoReader(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)

	oiByRevision := make(map[string]*ObjectInfo)
	for _, revision := range []string{
		"refs/heads/master",
		"refs/heads/master^{tree}",
		"refs/heads/master:README",
		"refs/tags/v1.1.1",
	} {
		revParseOutput := gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", revision)
		objectID, err := git.NewObjectIDFromHex(text.ChompBytes(revParseOutput))
		require.NoError(t, err)

		objectType := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "cat-file", "-t", revision))
		objectContents := gittest.Exec(t, cfg, "-C", repoPath, "cat-file", objectType, revision)

		oiByRevision[revision] = &ObjectInfo{
			Oid:  objectID,
			Type: objectType,
			Size: int64(len(objectContents)),
		}
	}

	cache := NewCache(cfg)

	for _, tc := range []struct {
		desc         string
		revision     git.Revision
		expectedErr  error
		expectedInfo *ObjectInfo
	}{
		{
			desc:         "commit by ref",
			revision:     "refs/heads/master",
			expectedInfo: oiByRevision["refs/heads/master"],
		},
		{
			desc:         "commit by ID",
			revision:     oiByRevision["refs/heads/master"].Oid.Revision(),
			expectedInfo: oiByRevision["refs/heads/master"],
		},
		{
			desc:         "tree",
			revision:     oiByRevision["refs/heads/master^{tree}"].Oid.Revision(),
			expectedInfo: oiByRevision["refs/heads/master^{tree}"],
		},
		{
			desc:         "blob",
			revision:     oiByRevision["refs/heads/master:README"].Oid.Revision(),
			expectedInfo: oiByRevision["refs/heads/master:README"],
		},
		{
			desc:         "tag",
			revision:     oiByRevision["refs/tags/v1.1.1"].Oid.Revision(),
			expectedInfo: oiByRevision["refs/tags/v1.1.1"],
		},
		{
			desc:        "nonexistent ref",
			revision:    "refs/heads/does-not-exist",
			expectedErr: NotFoundError{fmt.Errorf("object not found")},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			reader, err := cache.newObjectInfoReader(ctx, newRepoExecutor(t, cfg, repoProto))
			require.NoError(t, err)

			info, err := reader.info(tc.revision)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedInfo, info)

			// Verify that we do another request no matter whether the previous call
			// succeeded or failed.
			_, err = reader.info("refs/heads/master")
			require.NoError(t, err)
		})
	}
}
