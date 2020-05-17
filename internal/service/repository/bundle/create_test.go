package bundle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCreateFull(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, bundleRefWriter, err := Create(ctx, testRepo)
	require.NoError(t, err)
	require.NoError(t, bundleRefWriter.Commit())
}

func TestCreateIncremental(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, bundleRefWriter, err := Create(ctx, testRepo)
	require.NoError(t, err)
	require.NoError(t, bundleRefWriter.Commit())

	b, _, err := Create(ctx, testRepo)
	require.NoError(t, err)

	require.Nil(t, b)
}

func TestUnpack(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	b, bundleRefWriter, err := Create(ctx, testRepo)
	require.NoError(t, err)
	require.NoError(t, bundleRefWriter.Commit())

	emptyRepo, _, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	require.NoError(t, Unpack(ctx, emptyRepo, b))
}

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}
