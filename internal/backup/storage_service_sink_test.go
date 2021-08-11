package backup

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	_ "gocloud.dev/blob/memblob"
)

func TestStorageServiceSink(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	sss, err := NewStorageServiceSink(ctx, "mem://test_bucket")
	require.NoError(t, err)
	defer func() { require.NoError(t, sss.Close()) }()

	t.Run("write and retrieve", func(t *testing.T) {
		const relativePath = "path/to/data"

		data := []byte("test")

		require.NoError(t, sss.Write(ctx, relativePath, bytes.NewReader(data)))

		reader, err := sss.GetReader(ctx, relativePath)
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		retrieved, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, data, retrieved)
	})

	t.Run("not existing path", func(t *testing.T) {
		reader, err := sss.GetReader(ctx, "not-existing")
		require.Equal(t, fmt.Errorf(`storage service sink: new reader for "not-existing": %w`, ErrDoesntExist), err)
		require.Nil(t, reader)
	})

	t.Run("List", func(t *testing.T) {
		data := []byte("test")

		for _, relativePath := range []string{
			"a/a_pineapple",
			"b/a_apple",
			"b/a_carrot",
			"b/a_cucumber",
		} {
			require.NoError(t, sss.Write(ctx, relativePath, bytes.NewReader(data)))
		}

		expectedPaths := []string{
			"b/a_carrot",
			"b/a_cucumber",
		}

		paths, err := sss.List(ctx, "b/a_c")
		require.NoError(t, err)

		require.ElementsMatch(t, expectedPaths, paths)
	})
}
