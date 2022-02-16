package localrepo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Repo represents a local Git repository.
type Repo struct {
	repository.GitRepo
	locator       storage.Locator
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
}

// New creates a new Repo from its protobuf representation.
func New(locator storage.Locator, gitCmdFactory git.CommandFactory, catfileCache catfile.Cache, repo repository.GitRepo) *Repo {
	return &Repo{
		GitRepo:       repo,
		locator:       locator,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
	}
}

// NewTestRepo constructs a Repo. It is intended as a helper function for tests which assembles
// dependencies ad-hoc from the given config.
func NewTestRepo(t testing.TB, cfg config.Cfg, repo repository.GitRepo, factoryOpts ...git.ExecCommandFactoryOption) *Repo {
	t.Helper()

	if cfg.SocketPath != "it is a stub to bypass Validate method" {
		repo = gittest.RewrittenRepository(testhelper.Context(t), t, cfg, &gitalypb.Repository{
			StorageName:                   repo.GetStorageName(),
			RelativePath:                  repo.GetRelativePath(),
			GitObjectDirectory:            repo.GetGitObjectDirectory(),
			GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
		})
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, factoryOpts...)
	t.Cleanup(cleanup)
	require.NoError(t, err)

	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)

	locator := config.NewLocator(cfg)

	return New(locator, gitCmdFactory, catfileCache, repo)
}

// Exec creates a git command with the given args and Repo, executed in the
// Repo. It validates the arguments in the command before executing.
func (repo *Repo) Exec(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return repo.gitCmdFactory.New(ctx, repo, cmd, opts...)
}

// ExecAndWait is similar to Exec, but waits for the command to exit before
// returning.
func (repo *Repo) ExecAndWait(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) error {
	command, err := repo.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}

	return command.Wait()
}

// GitVersion returns the Git version in use.
func (repo *Repo) GitVersion(ctx context.Context) (git.Version, error) {
	return repo.gitCmdFactory.GitVersion(ctx)
}

func errorWithStderr(err error, stderr []byte) error {
	if len(stderr) == 0 {
		return err
	}
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}
