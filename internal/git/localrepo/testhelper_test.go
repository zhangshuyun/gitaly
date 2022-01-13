package localrepo

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

type setupRepoConfig struct {
	// emptyRepo will cause `setupRepo()` to create a new, empty repository instead of
	// cloning our test repository.
	emptyRepo bool
	// disableHooks will disable the use of hooks.
	disableHooks bool
}

type setupRepoOption func(*setupRepoConfig)

func withEmptyRepo() setupRepoOption {
	return func(cfg *setupRepoConfig) {
		cfg.emptyRepo = true
	}
}

func withDisabledHooks() setupRepoOption {
	return func(cfg *setupRepoConfig) {
		cfg.disableHooks = true
	}
}

func setupRepo(t *testing.T, opts ...setupRepoOption) (*Repo, string) {
	t.Helper()

	var setupRepoCfg setupRepoConfig
	for _, opt := range opts {
		opt(&setupRepoCfg)
	}

	cfg := testcfg.Build(t)

	var commandFactoryOpts []git.ExecCommandFactoryOption
	if setupRepoCfg.disableHooks {
		commandFactoryOpts = append(commandFactoryOpts, git.WithSkipHooks())
	}

	var repoProto *gitalypb.Repository
	var repoPath string
	if setupRepoCfg.emptyRepo {
		repoProto, repoPath = gittest.InitRepo(t, cfg, cfg.Storages[0])
	} else {
		repoProto, repoPath = gittest.CloneRepo(t, cfg, cfg.Storages[0])
	}

	gitCmdFactory := gittest.NewCommandFactory(t, cfg, commandFactoryOpts...)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	return New(gitCmdFactory, catfileCache, repoProto, cfg), repoPath
}
