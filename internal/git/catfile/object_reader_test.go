package catfile

import (
	"io"
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

func TestObjectReader_reader(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)

	commitID, err := git.NewObjectIDFromHex(text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master")))
	require.NoError(t, err)
	commitContents := gittest.Exec(t, cfg, "-C", repoPath, "cat-file", "-p", "refs/heads/master")

	t.Run("read existing object by ref", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), nil)
		require.NoError(t, err)

		object, err := reader.Object(ctx, "refs/heads/master")
		require.NoError(t, err)

		data, err := io.ReadAll(object)
		require.NoError(t, err)
		require.Equal(t, commitContents, data)
	})

	t.Run("read existing object by object ID", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), nil)
		require.NoError(t, err)

		object, err := reader.Object(ctx, commitID.Revision())
		require.NoError(t, err)

		data, err := io.ReadAll(object)
		require.NoError(t, err)

		require.Contains(t, string(data), "Merge branch 'cherry-pick-ce369011' into 'master'\n")
	})

	t.Run("read missing ref", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), nil)
		require.NoError(t, err)

		_, err = reader.Object(ctx, "refs/heads/does-not-exist")
		require.EqualError(t, err, "object not found")

		// Verify that we're still able to read a commit after the previous read has failed.
		object, err := reader.Object(ctx, commitID.Revision())
		require.NoError(t, err)

		data, err := io.ReadAll(object)
		require.NoError(t, err)

		require.Equal(t, commitContents, data)
	})

	t.Run("read fails when not consuming previous object", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), nil)
		require.NoError(t, err)

		_, err = reader.Object(ctx, commitID.Revision())
		require.NoError(t, err)

		// We haven't yet consumed the previous object, so this must now fail.
		_, err = reader.Object(ctx, commitID.Revision())
		require.EqualError(t, err, "current object has not been fully read")
	})

	t.Run("read fails when partially consuming previous object", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), nil)
		require.NoError(t, err)

		object, err := reader.Object(ctx, commitID.Revision())
		require.NoError(t, err)

		_, err = io.CopyN(io.Discard, object, 100)
		require.NoError(t, err)

		// We haven't yet consumed the previous object, so this must now fail.
		_, err = reader.Object(ctx, commitID.Revision())
		require.EqualError(t, err, "current object has not been fully read")
	})

	t.Run("read increments Prometheus counter", func(t *testing.T) {
		counter := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"type"})

		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto), counter)
		require.NoError(t, err)

		for objectType, revision := range map[string]git.Revision{
			"commit": "refs/heads/master",
			"tree":   "refs/heads/master^{tree}",
			"blob":   "refs/heads/master:README",
			"tag":    "refs/tags/v1.1.1",
		} {
			require.Equal(t, float64(0), testutil.ToFloat64(counter.WithLabelValues(objectType)))

			object, err := reader.Object(ctx, revision)
			require.NoError(t, err)

			require.Equal(t, float64(1), testutil.ToFloat64(counter.WithLabelValues(objectType)))

			_, err = io.Copy(io.Discard, object)
			require.NoError(t, err)
		}
	})
}
