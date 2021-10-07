package catfile

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestParseObjectInfoSuccess(t *testing.T) {
	testCases := []struct {
		desc     string
		input    string
		output   *ObjectInfo
		notFound bool
	}{
		{
			desc:  "existing object",
			input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222\n",
			output: &ObjectInfo{
				Oid:  "7c9373883988204e5a9f72c4a5119cbcefc83627",
				Type: "commit",
				Size: 222,
			},
		},
		{
			desc:     "non existing object",
			input:    "bla missing\n",
			notFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tc.input))
			output, err := ParseObjectInfo(reader)
			if tc.notFound {
				require.True(t, IsNotFound(err), "expect NotFoundError")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.output, output)
		})
	}
}

func TestParseObjectInfoErrors(t *testing.T) {
	testCases := []struct {
		desc  string
		input string
	}{
		{desc: "missing newline", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222"},
		{desc: "too few words", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit\n"},
		{desc: "too many words", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit 222 bla\n"},
		{desc: "parse object size", input: "7c9373883988204e5a9f72c4a5119cbcefc83627 commit bla\n"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bufio.NewReader(strings.NewReader(tc.input))
			_, err := ParseObjectInfo(reader)

			require.Error(t, err)
		})
	}
}

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
			counter := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"type"})

			reader, err := newObjectInfoReader(ctx, newRepoExecutor(t, cfg, repoProto), counter)
			require.NoError(t, err)

			require.Equal(t, float64(0), testutil.ToFloat64(counter.WithLabelValues("info")))

			info, err := reader.Info(ctx, tc.revision)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedInfo, info)

			require.Equal(t, float64(1), testutil.ToFloat64(counter.WithLabelValues("info")))

			// Verify that we do another request no matter whether the previous call
			// succeeded or failed.
			_, err = reader.Info(ctx, "refs/heads/master")
			require.NoError(t, err)

			require.Equal(t, float64(2), testutil.ToFloat64(counter.WithLabelValues("info")))
		})
	}
}
