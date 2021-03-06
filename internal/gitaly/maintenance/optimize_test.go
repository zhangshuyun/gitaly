package maintenance

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type mockOptimizer struct {
	t      testing.TB
	actual []*gitalypb.Repository
	cfg    config.Cfg
}

func (mo *mockOptimizer) OptimizeRepository(ctx context.Context, req *gitalypb.OptimizeRepositoryRequest, _ ...grpc.CallOption) (*gitalypb.OptimizeRepositoryResponse, error) {
	mo.actual = append(mo.actual, req.Repository)
	l := config.NewLocator(mo.cfg)
	gitCmdFactory := gittest.NewCommandFactory(mo.t, mo.cfg)
	catfileCache := catfile.NewCache(mo.cfg)
	mo.t.Cleanup(catfileCache.Stop)
	git2goExecutor := git2go.NewExecutor(mo.cfg, gitCmdFactory, l)
	txManager := transaction.NewManager(mo.cfg, backchannel.NewRegistry())
	housekeepingManager := housekeeping.NewManager(txManager)

	connsPool := client.NewPool()
	mo.t.Cleanup(func() { testhelper.MustClose(mo.t, connsPool) })

	resp, err := repository.NewServer(mo.cfg, nil, l,
		txManager,
		gitCmdFactory,
		catfileCache,
		connsPool,
		git2goExecutor,
		housekeepingManager,
	).OptimizeRepository(ctx, req)
	assert.NoError(mo.t, err)
	return resp, err
}

func TestOptimizeReposRandomly(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("0", "1", "2"))
	cfg := cfgBuilder.Build(t)

	for _, storage := range cfg.Storages {
		gittest.Exec(t, cfg, "init", "--bare", filepath.Join(storage.Path, "a"))
		gittest.Exec(t, cfg, "init", "--bare", filepath.Join(storage.Path, "b"))
	}

	cfg.Storages = append(cfg.Storages, config.Storage{
		Name: "duplicate",
		Path: cfg.Storages[0].Path,
	})
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc     string
		storages []string
		expected []*gitalypb.Repository
	}{
		{
			desc:     "two storages",
			storages: []string{"0", "1"},
			expected: []*gitalypb.Repository{
				{RelativePath: "a", StorageName: "0"},
				{RelativePath: "a", StorageName: "1"},
				{RelativePath: "b", StorageName: "0"},
				{RelativePath: "b", StorageName: "1"},
			},
		},
		{
			desc:     "duplicate storages",
			storages: []string{"0", "1", "duplicate"},
			expected: []*gitalypb.Repository{
				{RelativePath: "a", StorageName: "0"},
				{RelativePath: "a", StorageName: "1"},
				{RelativePath: "b", StorageName: "0"},
				{RelativePath: "b", StorageName: "1"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tickerDone := false
			tickerCount := 0

			ticker := helper.NewManualTicker()
			ticker.ResetFunc = func() {
				tickerCount++
				ticker.Tick()
			}
			ticker.StopFunc = func() {
				tickerDone = true
			}

			mo := &mockOptimizer{
				t:   t,
				cfg: cfg,
			}
			walker := OptimizeReposRandomly(cfg.Storages, mo, ticker, rand.New(rand.NewSource(1)))

			require.NoError(t, walker(ctx, testhelper.NewDiscardingLogEntry(t), tc.storages))
			require.ElementsMatch(t, tc.expected, mo.actual)
			require.True(t, tickerDone)
			// We expect one more tick than optimized repositories because of the
			// initial tick up front to re-start the timer.
			require.Equal(t, len(tc.expected)+1, tickerCount)
		})
	}
}

type mockOptimizerCancel struct {
	t         *testing.T
	startedAt time.Time
}

func (m mockOptimizerCancel) OptimizeRepository(ctx context.Context, _ *gitalypb.OptimizeRepositoryRequest, _ ...grpc.CallOption) (*gitalypb.OptimizeRepositoryResponse, error) {
	timeline, ok := ctx.Deadline()
	if assert.True(m.t, ok) {
		assert.True(m.t, timeline.After(m.startedAt), m.startedAt)
		future := m.startedAt.Add(10 * time.Minute)
		assert.True(m.t, timeline.Before(future), future)
	}
	return &gitalypb.OptimizeRepositoryResponse{}, nil
}

func TestOptimizeReposRandomly_cancellationOverride(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder()
	cfg := cfgBuilder.Build(t)

	gittest.InitRepo(t, cfg, cfg.Storages[0])
	ctx := testhelper.Context(t)

	// The timeout should be overwritten by the default 5 min timeout.
	//nolint:forbidigo // We're explicitly testing deadline override.
	ctx, cancel := context.WithTimeout(ctx, 72*time.Hour)
	defer cancel()

	ticker := helper.NewManualTicker()
	ticker.Tick()

	mo := &mockOptimizerCancel{t: t, startedAt: time.Now()}
	walker := OptimizeReposRandomly(cfg.Storages, mo, ticker, rand.New(rand.NewSource(1)))

	require.NoError(t, walker(ctx, testhelper.NewDiscardingLogEntry(t), []string{cfg.Storages[0].Name}))
}
