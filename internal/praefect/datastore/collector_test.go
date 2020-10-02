package datastore

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

type primaryGetterFunc func(context.Context) (map[string]string, error)

func (pg primaryGetterFunc) GetPrimaries(ctx context.Context) (map[string]string, error) {
	return pg(ctx)
}

func TestRepositoryStoreCollector(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	rs := NewMemoryRepositoryStore(nil)
	require.NoError(t, rs.SetGeneration(ctx, "some-read-only", "read-only", "secondary", 1))
	require.NoError(t, rs.SetGeneration(ctx, "some-read-only", "read-only", "primary", 0))
	require.NoError(t, rs.SetGeneration(ctx, "no-records", "writable", "primary", 0))
	require.NoError(t, rs.SetGeneration(ctx, "no-primary", "read-only", "primary", 0))

	c := NewRepositoryStoreCollector(nil, rs, primaryGetterFunc(func(context.Context) (map[string]string, error) {
		return map[string]string{
			"some-read-only": "primary",
			"all-writable":   "primary",
			"no-records":     "primary",
			"no-primary":     "",
		}, nil
	}))

	require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(`
# HELP gitaly_praefect_read_only_repositories Number of repositories in read-only mode within a virtual storage.
# TYPE gitaly_praefect_read_only_repositories gauge
gitaly_praefect_read_only_repositories{virtual_storage="some-read-only"} 1
gitaly_praefect_read_only_repositories{virtual_storage="all-writable"} 0
gitaly_praefect_read_only_repositories{virtual_storage="no-records"} 0
gitaly_praefect_read_only_repositories{virtual_storage="no-primary"} 1
`)))
}
