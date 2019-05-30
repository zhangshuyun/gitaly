package cache_test

import (
	"context"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

func TestStreamDBNaiveKeyer(t *testing.T) {
	keyer := cache.NaiveKeyer{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = setMockMethodCtx(ctx, "InfoRefsUploadPack")

	db, cleanup := tempDB(t, keyer)
	defer cleanup()

	req1 := &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			RelativePath: "DEADBEEF",
			StorageName:  config.Config.Storages[0].Name,
		},
	}

	_, err := db.GetStream(ctx, req1.Repository, req1)
	require.Error(t, err, "no streams have been created yet")

	expectStream1 := "this is a very cacheable stream"

	require.NoError(t, keyer.StartCriticalSection(req1.Repository))
	require.NoError(t, db.PutStream(ctx, req1.Repository, req1, strings.NewReader(expectStream1)))
	require.NoError(t, keyer.EndCriticalSection(req1.Repository))

	stream1, err := db.GetStream(ctx, req1.Repository, req1)
	require.NoError(t, err)

	out, err := ioutil.ReadAll(stream1)
	require.NoError(t, err)
	require.Equal(t, expectStream1, string(out))

	// invalidate repo:
	require.NoError(t, keyer.StartCriticalSection(req1.Repository))
	expectStream2 := "not what you were looking for"
	require.NoError(t, db.PutStream(ctx, req1.Repository, req1, strings.NewReader(expectStream2)))
	require.NoError(t, keyer.EndCriticalSection(req1.Repository))

	stream2, err := db.GetStream(ctx, req1.Repository, req1)
	require.NoError(t, err)

	out2, err := ioutil.ReadAll(stream2)
	require.NoError(t, err)
	require.Equal(t, expectStream2, string(out2))
}

func TestStreamDBMultiProc(t *testing.T) {

}
