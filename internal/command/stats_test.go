package command

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestStatsFromContext_BackgroundContext(t *testing.T) {
	ctx := testhelper.Context(t)

	stats := StatsFromContext(ctx)
	require.Nil(t, stats)
}

func TestStatsFromContext_InitContext(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = InitContextStats(ctx)

	stats := StatsFromContext(ctx)

	require.NotNil(t, stats)
	require.Equal(t, stats.Fields(), logrus.Fields{})
}

func TestStatsFromContext_RecordSum(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = InitContextStats(ctx)

	stats := StatsFromContext(ctx)

	stats.RecordSum("foo", 1)
	stats.RecordSum("foo", 1)

	require.NotNil(t, stats)
	require.Equal(t, stats.Fields(), logrus.Fields{"foo": 2})
}

func TestStatsFromContext_RecordSumByRef(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = InitContextStats(ctx)

	stats := StatsFromContext(ctx)

	stats.RecordSum("foo", 1)
	stats.RecordSum("foo", 1)

	stats2 := StatsFromContext(ctx)

	require.NotNil(t, stats2)
	require.Equal(t, stats2.Fields(), logrus.Fields{"foo": 2})
}

func TestStatsFromContext_RecordMax(t *testing.T) {
	ctx := testhelper.Context(t)

	ctx = InitContextStats(ctx)

	stats := StatsFromContext(ctx)

	stats.RecordMax("foo", 1024)
	stats.RecordMax("foo", 256)
	stats.RecordMax("foo", 512)

	require.NotNil(t, stats)
	require.Equal(t, stats.Fields(), logrus.Fields{"foo": 1024})
}
