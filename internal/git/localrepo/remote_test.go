package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestRepo_FetchRemote(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	gitCmdFactory := gittest.NewCommandFactory(t, cfg, git.WithSkipHooks())
	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()
	locator := config.NewLocator(cfg)

	initBareWithRemote := func(t *testing.T, remote string) (*Repo, string) {
		t.Helper()

		_, remoteRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		clientRepo, clientRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

		cmd := gittest.NewCommand(t, cfg, "-C", clientRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			require.NoError(t, err)
		}

		return New(locator, gitCmdFactory, catfileCache, clientRepo), clientRepoPath
	}

	t.Run("invalid name", func(t *testing.T) {
		repo := New(locator, gitCmdFactory, catfileCache, nil)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, git.ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])

		repo := New(locator, gitCmdFactory, catfileCache, repoProto)
		var stderr bytes.Buffer
		err := repo.FetchRemote(ctx, "stub", FetchOpts{Stderr: &stderr})
		require.Error(t, err)
		require.Contains(t, stderr.String(), "'stub' does not appear to be a git repository")
	})

	t.Run("ok", func(t *testing.T) {
		repo, testRepoPath := initBareWithRemote(t, "origin")

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Stderr: &stderr}))

		require.Empty(t, stderr.String(), "it should not produce output as it is called with --quite flag by default")

		fetchHeadData := testhelper.MustReadFile(t, filepath.Join(testRepoPath, "FETCH_HEAD"))

		fetchHead := string(fetchHeadData)
		require.Contains(t, fetchHead, "e56497bb5f03a90a51293fc6d516788730953899	not-for-merge	branch ''test''")
		require.Contains(t, fetchHead, "8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b	not-for-merge	tag 'v1.1.0'")

		sha, err := repo.ResolveRevision(ctx, git.Revision("refs/remotes/origin/master^{commit}"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312"), sha)
	})

	t.Run("with env", func(t *testing.T) {
		_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		repo := New(locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --quiet --atomic --end-of-options source")
	})

	t.Run("with disabled transactions", func(t *testing.T) {
		_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		repo := New(locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{
			Stderr:              &stderr,
			Env:                 []string{"GIT_TRACE=1"},
			DisableTransactions: true,
		}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --quiet --end-of-options source")
	})

	t.Run("with globals", func(t *testing.T) {
		_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		repo := New(locator, gitCmdFactory, catfileCache, testRepo)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		gittest.Exec(t, cfg, "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		gittest.Exec(t, cfg, "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(
			ctx,
			"source",
			FetchOpts{
				CommandOptions: []git.CmdOpt{
					git.WithConfig(git.ConfigPair{Key: "fetch.prune", Value: "true"}),
				},
			}),
		)

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with prune", func(t *testing.T) {
		_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		repo := New(locator, gitCmdFactory, catfileCache, testRepo)

		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		gittest.Exec(t, cfg, "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		gittest.Exec(t, cfg, "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Prune: true}))

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with no tags", func(t *testing.T) {
		repo, testRepoPath := initBareWithRemote(t, "origin")

		tagsBefore := gittest.Exec(t, cfg, "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsBefore)

		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Tags: FetchOptsTagsNone, Force: true}))

		tagsAfter := gittest.Exec(t, cfg, "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsAfter)

		containsBranches, err := repo.HasRevision(ctx, git.Revision("'test'"))
		require.NoError(t, err)
		require.False(t, containsBranches)

		containsTags, err := repo.HasRevision(ctx, git.Revision("v1.1.0"))
		require.NoError(t, err)
		require.False(t, containsTags)
	})

	t.Run("with invalid remote", func(t *testing.T) {
		repo, _ := initBareWithRemote(t, "origin")

		err := repo.FetchRemote(ctx, "doesnotexist", FetchOpts{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "fatal: 'doesnotexist' does not appear to be a git repository")
		require.IsType(t, err, ErrFetchFailed{})
	})
}

// captureGitSSHCommand creates a new intercepting command factory which captures the
// GIT_SSH_COMMAND environment variable. The returned function can be used to read the variables's
// value.
func captureGitSSHCommand(ctx context.Context, t testing.TB, cfg config.Cfg) (git.CommandFactory, func() ([]byte, error)) {
	envPath := filepath.Join(testhelper.TempDir(t), "GIT_SSH_PATH")

	gitCmdFactory := gittest.NewInterceptingCommandFactory(ctx, t, cfg, func(execEnv git.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			if test -z "${GIT_SSH_COMMAND+x}"
			then
				rm -f %q
			else
				echo -n "$GIT_SSH_COMMAND" >%q
			fi
			%q "$@"
		`, envPath, envPath, execEnv.BinaryPath)
	})

	return gitCmdFactory, func() ([]byte, error) {
		return os.ReadFile(envPath)
	}
}

func TestRepo_Push(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, sourceRepoPb, _ := testcfg.BuildWithRepo(t)

	gitCmdFactory, readSSHCommand := captureGitSSHCommand(ctx, t, cfg)
	catfileCache := catfile.NewCache(cfg)
	t.Cleanup(catfileCache.Stop)
	locator := config.NewLocator(cfg)

	sourceRepo := New(locator, gitCmdFactory, catfileCache, sourceRepoPb)

	setupPushRepo := func(t testing.TB) (*Repo, string, []git.ConfigPair) {
		repoProto, repopath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		return New(locator, gitCmdFactory, catfileCache, repoProto), repopath, nil
	}

	setupDivergedRepo := func(t testing.TB) (*Repo, string, []git.ConfigPair) {
		repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := New(locator, gitCmdFactory, catfileCache, repoProto)

		// set up master as a divergin ref in push repo
		sourceMaster, err := sourceRepo.GetReference(ctx, "refs/heads/master")
		require.NoError(t, err)

		require.NoError(t, sourceRepo.Push(ctx, repoPath, []string{"refs/*"}, PushOptions{}))
		divergedMaster := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithBranch("master"),
			gittest.WithParents(git.ObjectID(sourceMaster.Target)),
		)

		master, err := repo.GetReference(ctx, "refs/heads/master")
		require.NoError(t, err)
		require.Equal(t, master.Target, divergedMaster.String())

		return repo, repoPath, nil
	}

	for _, tc := range []struct {
		desc           string
		setupPushRepo  func(testing.TB) (*Repo, string, []git.ConfigPair)
		config         []git.ConfigPair
		sshCommand     string
		force          bool
		refspecs       []string
		errorMessage   string
		expectedFilter []string
	}{
		{
			desc:          "refspecs must be specified",
			setupPushRepo: setupPushRepo,
			errorMessage:  "refspecs to push must be explicitly specified",
		},
		{
			desc:           "push two refs",
			setupPushRepo:  setupPushRepo,
			refspecs:       []string{"refs/heads/master", "refs/heads/feature"},
			expectedFilter: []string{"refs/heads/master", "refs/heads/feature"},
		},
		{
			desc:           "push with custom ssh command",
			setupPushRepo:  setupPushRepo,
			sshCommand:     "custom --ssh-command",
			refspecs:       []string{"refs/heads/master"},
			expectedFilter: []string{"refs/heads/master"},
		},
		{
			desc:          "doesn't force push over diverged refs with Force unset",
			refspecs:      []string{"refs/heads/master"},
			setupPushRepo: setupDivergedRepo,
			errorMessage:  "Updates were rejected because the remote contains work that you do",
		},
		{
			desc:          "force pushes over diverged refs with Force set",
			refspecs:      []string{"refs/heads/master"},
			force:         true,
			setupPushRepo: setupDivergedRepo,
		},
		{
			desc:          "push all refs",
			setupPushRepo: setupPushRepo,
			refspecs:      []string{"refs/*"},
		},
		{
			desc:          "push empty refspec",
			setupPushRepo: setupPushRepo,
			refspecs:      []string{""},
			errorMessage:  `git push: exit status 128, stderr: "fatal: invalid refspec ''\n"`,
		},
		{
			desc: "invalid remote",
			setupPushRepo: func(t testing.TB) (*Repo, string, []git.ConfigPair) {
				repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return New(locator, gitCmdFactory, catfileCache, repoProto), "", nil
			},
			refspecs:     []string{"refs/heads/master"},
			errorMessage: `git push: exit status 128, stderr: "fatal: no path specified; see 'git help pull' for valid url syntax\n"`,
		},
		{
			desc: "in-memory remote",
			setupPushRepo: func(testing.TB) (*Repo, string, []git.ConfigPair) {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return New(locator, gitCmdFactory, catfileCache, repoProto), "inmemory", []git.ConfigPair{
					{Key: "remote.inmemory.url", Value: repoPath},
				}
			},
			refspecs:       []string{"refs/heads/master"},
			expectedFilter: []string{"refs/heads/master"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			pushRepo, remote, remoteConfig := tc.setupPushRepo(t)

			err := sourceRepo.Push(ctx, remote, tc.refspecs, PushOptions{
				SSHCommand: tc.sshCommand,
				Force:      tc.force,
				Config:     remoteConfig,
			})
			if tc.errorMessage != "" {
				require.Contains(t, err.Error(), tc.errorMessage)
				return
			}
			require.NoError(t, err)

			gitSSHCommand, err := readSSHCommand()
			if !os.IsNotExist(err) {
				require.NoError(t, err)
			}

			require.Equal(t, tc.sshCommand, string(gitSSHCommand))

			actual, err := pushRepo.GetReferences(ctx)
			require.NoError(t, err)

			expected, err := sourceRepo.GetReferences(ctx, tc.expectedFilter...)
			require.NoError(t, err)

			require.Equal(t, expected, actual)
		})
	}
}
