package catfile

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"

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
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		object, err := reader.reader("refs/heads/master", "commit")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(object)
		require.NoError(t, err)
		require.Equal(t, commitContents, data)
	})

	t.Run("read existing object by object ID", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		object, err := reader.reader(commitID.Revision(), "commit")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(object)
		require.NoError(t, err)

		require.Contains(t, string(data), "Merge branch 'cherry-pick-ce369011' into 'master'\n")
	})

	t.Run("read commit with wrong type", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		_, err = reader.reader(commitID.Revision(), "tag")
		require.EqualError(t, err, fmt.Sprintf("expected %s to be a tag, got commit", commitID))

		// Verify that we're still able to read a commit after the previous read has failed.
		object, err := reader.reader(commitID.Revision(), "commit")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(object)
		require.NoError(t, err)

		require.Equal(t, commitContents, data)
	})

	t.Run("read missing ref", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		_, err = reader.reader("refs/heads/does-not-exist", "commit")
		require.EqualError(t, err, "object not found")

		// Verify that we're still able to read a commit after the previous read has failed.
		object, err := reader.reader(commitID.Revision(), "commit")
		require.NoError(t, err)

		data, err := ioutil.ReadAll(object)
		require.NoError(t, err)

		require.Equal(t, commitContents, data)
	})

	t.Run("read fails when not consuming previous object", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		_, err = reader.reader(commitID.Revision(), "commit")
		require.NoError(t, err)

		// We haven't yet consumed the previous object, so this must now fail.
		_, err = reader.reader(commitID.Revision(), "commit")
		require.EqualError(t, err, fmt.Sprintf("cannot create new Object: batch contains %d unread bytes", len(commitContents)+1))
	})

	t.Run("read fails when partially consuming previous object", func(t *testing.T) {
		reader, err := newObjectReader(ctx, newRepoExecutor(t, cfg, repoProto))
		require.NoError(t, err)

		object, err := reader.reader(commitID.Revision(), "commit")
		require.NoError(t, err)

		_, err = io.CopyN(ioutil.Discard, object, 100)
		require.NoError(t, err)

		// We haven't yet consumed the previous object, so this must now fail.
		_, err = reader.reader(commitID.Revision(), "commit")
		require.EqualError(t, err, fmt.Sprintf("cannot create new Object: batch contains %d unread bytes", len(commitContents)-100+1))
	})
}
