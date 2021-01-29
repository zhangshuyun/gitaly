package maintenance

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type mockOptimizer struct {
	t        testing.TB
	actual   []*gitalypb.Repository
	storages []config.Storage
}

func (mo *mockOptimizer) OptimizeRepository(ctx context.Context, req *gitalypb.OptimizeRepositoryRequest, _ ...grpc.CallOption) (*gitalypb.OptimizeRepositoryResponse, error) {
	mo.actual = append(mo.actual, req.Repository)
	cfg := config.Cfg{Storages: mo.storages}
	l := config.NewLocator(cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	resp, err := repository.NewServer(cfg, nil, l, transaction.NewManager(cfg), gitCmdFactory).OptimizeRepository(ctx, req)
	assert.NoError(mo.t, err)
	return resp, err
}

func TestOptimizeReposRandomly(t *testing.T) {
	oldStorages := config.Config.Storages
	defer func() { config.Config.Storages = oldStorages }()

	storages := []config.Storage{}

	for i := 0; i < 3; i++ {
		tempDir, cleanup := testhelper.TempDir(t)
		defer cleanup()

		storages = append(storages, config.Storage{
			Name: strconv.Itoa(i),
			Path: tempDir,
		})

		testhelper.MustRunCommand(t, nil, "git", "init", "--bare", filepath.Join(tempDir, "a"))
		testhelper.MustRunCommand(t, nil, "git", "init", "--bare", filepath.Join(tempDir, "b"))
	}

	config.Config.Storages = storages

	mo := &mockOptimizer{
		t:        t,
		storages: storages,
	}
	walker := OptimizeReposRandomly(storages, mo)

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, walker(ctx, testhelper.DiscardTestEntry(t), []string{"0", "1"}))

	expect := []*gitalypb.Repository{
		{RelativePath: "a", StorageName: storages[0].Name},
		{RelativePath: "a", StorageName: storages[1].Name},
		{RelativePath: "b", StorageName: storages[0].Name},
		{RelativePath: "b", StorageName: storages[1].Name},
	}
	require.ElementsMatch(t, expect, mo.actual)

	// repeat storage paths should not impact repos visited
	storages = append(storages, config.Storage{
		Name: "duplicate",
		Path: storages[0].Path,
	})

	mo = &mockOptimizer{
		t:        t,
		storages: storages,
	}
	config.Config.Storages = storages

	walker = OptimizeReposRandomly(storages, mo)
	require.NoError(t, walker(ctx, testhelper.DiscardTestEntry(t), []string{"0", "1", "duplicate"}))
	require.Equal(t, len(expect), len(mo.actual))
}
