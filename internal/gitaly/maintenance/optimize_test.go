package maintenance

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
	gitCmdFactory := git.NewExecCommandFactory(mo.cfg)
	resp, err := repository.NewServer(mo.cfg, nil, l, transaction.NewManager(mo.cfg), gitCmdFactory).OptimizeRepository(ctx, req)
	assert.NoError(mo.t, err)
	return resp, err
}

func TestOptimizeReposRandomly(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("0", "1", "2"))
	defer cfgBuilder.Cleanup()
	cfg := cfgBuilder.Build(t)

	for _, storage := range cfg.Storages {
		testhelper.MustRunCommand(t, nil, "git", "init", "--bare", filepath.Join(storage.Path, "a"))
		testhelper.MustRunCommand(t, nil, "git", "init", "--bare", filepath.Join(storage.Path, "b"))
	}

	mo := &mockOptimizer{
		t:   t,
		cfg: cfg,
	}
	walker := OptimizeReposRandomly(cfg.Storages, mo)

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, walker(ctx, testhelper.DiscardTestEntry(t), []string{"0", "1"}))

	expect := []*gitalypb.Repository{
		{RelativePath: "a", StorageName: cfg.Storages[0].Name},
		{RelativePath: "a", StorageName: cfg.Storages[1].Name},
		{RelativePath: "b", StorageName: cfg.Storages[0].Name},
		{RelativePath: "b", StorageName: cfg.Storages[1].Name},
	}
	require.ElementsMatch(t, expect, mo.actual)

	// repeat storage paths should not impact repos visited
	cfg.Storages = append(cfg.Storages, config.Storage{
		Name: "duplicate",
		Path: cfg.Storages[0].Path,
	})

	mo = &mockOptimizer{
		t:   t,
		cfg: cfg,
	}

	walker = OptimizeReposRandomly(cfg.Storages, mo)
	require.NoError(t, walker(ctx, testhelper.DiscardTestEntry(t), []string{"0", "1", "duplicate"}))
	require.Equal(t, len(expect), len(mo.actual))
}
