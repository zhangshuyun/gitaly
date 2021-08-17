package backup

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
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

func testSinkList(ctx context.Context, t *testing.T, s Sink) {
	data := []byte("test")

	for _, relativePath := range []string{
		"a/a_pineapple",
		"b/a_apple",
		"b/a_carrot",
		"b/a_cucumber",
		"b/banana/a_fruit",
	} {
		require.NoError(t, s.Write(ctx, relativePath, bytes.NewReader(data)))
	}

	expectedPaths := []string{
		"b/a_carrot",
		"b/a_cucumber",
	}

	paths, err := s.List(ctx, "b/a_c")
	require.NoError(t, err)

	require.ElementsMatch(t, expectedPaths, paths)
}
