package bundle

import (
	"bytes"
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

	_, err := Create(ctx, testRepo)
	require.NoError(t, err)
}

func TestCreateIncremental(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := Create(ctx, testRepo)
	require.NoError(t, err)

	b, err := Create(ctx, testRepo)
	require.NoError(t, err)

	bytes := bytes.SplitN(b, []byte("\n\n"), 2)
	require.Empty(t, bytes[1])
}

func TestUnpack(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	b, err := Create(ctx, testRepo)
	require.NoError(t, err)

	emptyRepo, emptyRepoPath, _ := testhelper.InitBareRepo(t)
	//	defer cleanup()

	t.Logf("EMPTY REPO: %v\n", emptyRepoPath)
	require.NoError(t, Unpack(ctx, emptyRepo, b))
}

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}
