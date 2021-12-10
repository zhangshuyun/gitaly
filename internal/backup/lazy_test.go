package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestLazyWrite_noData(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	var called bool
	sink := MockSink{
		WriteFn: func(ctx context.Context, relativePath string, r io.Reader) error {
			called = true
			return nil
		},
	}

	err := LazyWrite(ctx, sink, "a-file", strings.NewReader(""))
	require.NoError(t, err)
	require.False(t, called)
}

func TestLazyWrite_data(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	expectedData := make([]byte, 512)
	_, err := rand.Read(expectedData)
	require.NoError(t, err)

	var data bytes.Buffer

	sink := MockSink{
		WriteFn: func(ctx context.Context, relativePath string, r io.Reader) error {
			_, err := io.Copy(&data, r)
			return err
		},
	}

	require.NoError(t, LazyWrite(ctx, sink, "a-file", bytes.NewReader(expectedData)))
	require.Equal(t, expectedData, data.Bytes())
}

type MockSink struct {
	GetReaderFn func(ctx context.Context, relativePath string) (io.ReadCloser, error)
	WriteFn     func(ctx context.Context, relativePath string, r io.Reader) error
}

func (s MockSink) Write(ctx context.Context, relativePath string, r io.Reader) error {
	if s.WriteFn != nil {
		return s.WriteFn(ctx, relativePath, r)
	}
	return nil
}

func (s MockSink) GetReader(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	if s.GetReaderFn != nil {
		return s.GetReaderFn(ctx, relativePath)
	}
	return io.NopCloser(strings.NewReader("")), nil
}
