package localrepo

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func setupRepoRemote(t *testing.T, bare bool) (Remote, string) {
	t.Helper()

	cfg := testcfg.Build(t)

	cfg.Ruby.Dir = "/var/empty"

	var repoProto *gitalypb.Repository
	var repoPath string
	if bare {
		repoProto, repoPath = gittest.InitRepo(t, cfg, cfg.Storages[0])
	} else {
		repoProto, repoPath = gittest.CloneRepo(t, cfg, cfg.Storages[0])
	}

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	return New(gitCmdFactory, catfile.NewCache(cfg), repoProto, cfg).Remote(), repoPath
}

func TestRepo_Remote(t *testing.T) {
	repository := &gitalypb.Repository{StorageName: "stub", RelativePath: "/stub"}

	repo := New(nil, nil, repository, config.Cfg{})
	require.Equal(t, Remote{repo: repo}, repo.Remote())
}

func TestBuildRemoteAddOptsFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts git.RemoteAddOpts
		exp  []git.Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: git.RemoteAddOpts{
				Tags:                   git.RemoteAddOptsTagsNone,
				Fetch:                  true,
				RemoteTrackingBranches: []string{"branch-1", "branch-2"},
				DefaultBranch:          "develop",
				Mirror:                 git.RemoteAddOptsMirrorPush,
			},
			exp: []git.Option{
				git.ValueFlag{Name: "-t", Value: "branch-1"},
				git.ValueFlag{Name: "-t", Value: "branch-2"},
				git.ValueFlag{Name: "-m", Value: "develop"},
				git.Flag{Name: "-f"},
				git.Flag{Name: "--no-tags"},
				git.ValueFlag{Name: "--mirror", Value: "push"},
			},
		},
		{
			desc: "with tags",
			opts: git.RemoteAddOpts{Tags: git.RemoteAddOptsTagsAll},
			exp:  []git.Option{git.Flag{Name: "--tags"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, buildRemoteAddOptsFlags(tc.opts))
		})
	}
}

func TestRemote_Add(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	remote, repoPath := setupRepoRemote(t, false)

	gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "remove", "origin")

	_, remoteRepoPath := gittest.CloneRepo(t, remote.repo.cfg, remote.repo.cfg.Storages[0])

	t.Run("invalid argument", func(t *testing.T) {
		for _, tc := range []struct {
			desc      string
			name, url string
			errMsg    string
		}{
			{
				desc:   "name",
				name:   " ",
				url:    "http://some.com.git",
				errMsg: `"name" is blank or empty`,
			},
			{
				desc:   "url",
				name:   "name",
				url:    "",
				errMsg: `"url" is blank or empty`,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				err := remote.Add(ctx, tc.name, tc.url, git.RemoteAddOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, git.ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("fetch", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "first", remoteRepoPath, git.RemoteAddOpts{Fetch: true}))

		remotes := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	"+remoteRepoPath+" (fetch)\n"+
				"first	"+remoteRepoPath+" (push)",
			remotes,
		)
		latestSHA := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "rev-parse", "refs/remotes/first/master"))
		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", latestSHA)
	})

	t.Run("default branch", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "second", "http://some.com.git", git.RemoteAddOpts{DefaultBranch: "wip"}))

		defaultRemote := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "symbolic-ref", "refs/remotes/second/HEAD"))
		require.Equal(t, "refs/remotes/second/wip", defaultRemote)
	})

	t.Run("remote tracking branches", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "third", "http://some.com.git", git.RemoteAddOpts{RemoteTrackingBranches: []string{"a", "b"}}))

		defaultRemote := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "config", "--get-all", "remote.third.fetch"))
		require.Equal(t, "+refs/heads/a:refs/remotes/third/a\n+refs/heads/b:refs/remotes/third/b", defaultRemote)
	})

	t.Run("already exists", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "fourth", "http://some.com.git", git.RemoteAddOpts{}))

		err := remote.Add(ctx, "fourth", "http://some.com.git", git.RemoteAddOpts{})
		require.Equal(t, git.ErrAlreadyExists, err)
	})
}

func TestRemote_Remove(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	remote, repoPath := setupRepoRemote(t, true)

	t.Run("ok", func(t *testing.T) {
		gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "add", "first", "http://some.com.git")

		require.NoError(t, remote.Remove(ctx, "first"))

		remotes := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "--verbose"))
		require.Empty(t, remotes)
	})

	t.Run("not found", func(t *testing.T) {
		err := remote.Remove(ctx, "second")
		require.Equal(t, git.ErrNotFound, err)
	})

	t.Run("invalid argument: name", func(t *testing.T) {
		err := remote.Remove(ctx, " ")
		require.Error(t, err)
		assert.True(t, errors.Is(err, git.ErrInvalidArg))
		assert.Contains(t, err.Error(), `"name" is blank or empty`)
	})

	t.Run("don't remove local branches", func(t *testing.T) {
		remote, repoPath := setupRepoRemote(t, false)

		// configure remote as fetch mirror
		gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "config", "remote.origin.fetch", "+refs/*:refs/*")
		gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "fetch")

		masterBeforeRemove := gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "show-ref", "refs/heads/master")

		require.NoError(t, remote.Remove(ctx, "origin"))

		out := gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote")
		require.Len(t, out, 0)

		out = gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "show-ref", "refs/heads/master")
		require.Equal(t, masterBeforeRemove, out)
	})
}

func TestRemote_Exists(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	remote, _ := setupRepoRemote(t, false)

	found, err := remote.Exists(ctx, "origin")
	require.NoError(t, err)
	require.True(t, found)

	found, err = remote.Exists(ctx, "can-not-be-found")
	require.NoError(t, err)
	require.False(t, found)
}

func TestBuildSetURLOptsFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts git.SetURLOpts
		exp  []git.Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: git.SetURLOpts{Push: true},
			exp:  []git.Option{git.Flag{Name: "--push"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, buildSetURLOptsFlags(tc.opts))
		})
	}
}

func TestRemote_SetURL(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	remote, repoPath := setupRepoRemote(t, true)

	t.Run("invalid argument", func(t *testing.T) {
		for _, tc := range []struct {
			desc      string
			name, url string
			errMsg    string
		}{
			{
				desc:   "name",
				name:   " ",
				url:    "http://some.com.git",
				errMsg: `"name" is blank or empty`,
			},
			{
				desc:   "url",
				name:   "name",
				url:    "",
				errMsg: `"url" is blank or empty`,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				err := remote.SetURL(ctx, tc.name, tc.url, git.SetURLOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, git.ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "add", "first", "file:/"+repoPath)

		require.NoError(t, remote.SetURL(ctx, "first", "http://some.com.git", git.SetURLOpts{Push: true}))

		remotes := text.ChompBytes(gittest.Exec(t, remote.repo.cfg, "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	file:/"+repoPath+" (fetch)\n"+
				"first	http://some.com.git (push)",
			remotes,
		)
	})

	t.Run("doesnt exist", func(t *testing.T) {
		err := remote.SetURL(ctx, "second", "http://some.com.git", git.SetURLOpts{})
		require.True(t, errors.Is(err, git.ErrNotFound), err)
	})
}

func TestRepo_FetchRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	remoteCmd, remoteRepoPath := setupRepoRemote(t, false)
	cfg := remoteCmd.repo.cfg

	initBareWithRemote := func(t *testing.T, remote string) (*Repo, string) {
		t.Helper()

		testRepo, testRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

		cmd := exec.Command(cfg.Git.BinPath, "-C", testRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			require.NoError(t, err)
		}

		return New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, testRepo, cfg), testRepoPath
	}

	t.Run("invalid name", func(t *testing.T) {
		repo := New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, nil, cfg)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, git.ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		repo := New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, remoteCmd.repo, cfg)
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

		repo := New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, testRepo, cfg)
		gittest.Exec(t, cfg, "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --quiet --end-of-options source")
	})

	t.Run("with globals", func(t *testing.T) {
		_, sourceRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
		testRepo, testRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		repo := New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, testRepo, cfg)
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

		repo := New(remoteCmd.repo.gitCmdFactory, remoteCmd.repo.catfileCache, testRepo, cfg)

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

// captureGitSSHCommand returns a path to a script that wraps the git at the passed in path
// and records the GIT_SSH_COMMAND that was passed to it. It returns also a path to the file
// that contains the captured GIT_SSH_COMMAND value.
func captureGitSSHCommand(t testing.TB, gitBinaryPath string) (string, string) {
	tmpDir := t.TempDir()

	gitPath := filepath.Join(tmpDir, "git-hook")
	envPath := filepath.Join(tmpDir, "GIT_SSH_PATH")
	require.NoError(t, ioutil.WriteFile(
		gitPath,
		[]byte(fmt.Sprintf(
			`#!/usr/bin/env bash
if [ -z ${GIT_SSH_COMMAND+x} ];then rm -f %q ;else echo -n "$GIT_SSH_COMMAND" > %q; fi
%s "$@"`,
			envPath, envPath,
			gitBinaryPath,
		)),
		os.ModePerm),
	)

	return gitPath, envPath
}

func TestRepo_Push(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, sourceRepoPb, _ := testcfg.BuildWithRepo(t)

	wrappedGit, gitSSHCommandFile := captureGitSSHCommand(t, cfg.Git.BinPath)

	cfg.Git.BinPath = wrappedGit
	sourceRepo := NewTestRepo(t, cfg, sourceRepoPb)

	setupPushRepo := func(t testing.TB) (*Repo, string, []git.ConfigPair) {
		repoProto, repopath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		return NewTestRepo(t, cfg, repoProto), repopath, nil
	}

	setupDivergedRepo := func(t testing.TB) (*Repo, string, []git.ConfigPair) {
		repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := NewTestRepo(t, cfg, repoProto)

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
				return NewTestRepo(t, cfg, repoProto), "", nil
			},
			refspecs:     []string{"refs/heads/master"},
			errorMessage: `git push: exit status 128, stderr: "fatal: no path specified; see 'git help pull' for valid url syntax\n"`,
		},
		{
			desc: "in-memory remote",
			setupPushRepo: func(testing.TB) (*Repo, string, []git.ConfigPair) {
				repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
				return NewTestRepo(t, cfg, repoProto), "inmemory", []git.ConfigPair{
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

			gitSSHCommand, err := os.ReadFile(gitSSHCommandFile)
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
