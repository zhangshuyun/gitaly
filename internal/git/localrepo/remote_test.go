package localrepo

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRepo_Remote(t *testing.T) {
	repository := &gitalypb.Repository{StorageName: "stub", RelativePath: "/stub"}

	repo := New(repository, config.Config)
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
	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := New(repoProto, config.Config)

	_, remoteRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "remove", "origin")

	ctx, cancel := testhelper.Context()
	defer cancel()

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	remote := repo.Remote()

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

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	"+remoteRepoPath+" (fetch)\n"+
				"first	"+remoteRepoPath+" (push)",
			remotes,
		)
		latestSHA := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "refs/remotes/first/master"))
		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", latestSHA)
	})

	t.Run("default branch", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "second", "http://some.com.git", git.RemoteAddOpts{DefaultBranch: "wip"}))

		defaultRemote := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "symbolic-ref", "refs/remotes/second/HEAD"))
		require.Equal(t, "refs/remotes/second/wip", defaultRemote)
	})

	t.Run("remote tracking branches", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "third", "http://some.com.git", git.RemoteAddOpts{RemoteTrackingBranches: []string{"a", "b"}}))

		defaultRemote := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--get-all", "remote.third.fetch"))
		require.Equal(t, "+refs/heads/a:refs/remotes/third/a\n+refs/heads/b:refs/remotes/third/b", defaultRemote)
	})

	t.Run("already exists", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "fourth", "http://some.com.git", git.RemoteAddOpts{}))

		err := remote.Add(ctx, "fourth", "http://some.com.git", git.RemoteAddOpts{})
		require.Equal(t, git.ErrAlreadyExists, err)
	})
}

func TestRemote_Remove(t *testing.T) {
	repoProto, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()
	repo := New(repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

	remote := repo.Remote()

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "add", "first", "http://some.com.git")

		require.NoError(t, remote.Remove(ctx, "first"))

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
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
	repoProto, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()
	repo := New(repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

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
				remote := repo.Remote()
				err := remote.SetURL(ctx, tc.name, tc.url, git.SetURLOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, git.ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "add", "first", "file:/"+repoPath)

		remote := Remote{repo: repo}
		require.NoError(t, remote.SetURL(ctx, "first", "http://some.com.git", git.SetURLOpts{Push: true}))

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	file:/"+repoPath+" (fetch)\n"+
				"first	http://some.com.git (push)",
			remotes,
		)
	})

	t.Run("doesnt exist", func(t *testing.T) {
		remote := Remote{repo: repo}
		err := remote.SetURL(ctx, "second", "http://some.com.git", git.SetURLOpts{})
		require.True(t, errors.Is(err, git.ErrNotFound), err)
	})
}

func TestRepo_FetchRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, remoteRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	initBareWithRemote := func(t *testing.T, remote string) (*Repo, string, testhelper.Cleanup) {
		t.Helper()

		testRepo, testRepoPath, cleanup := testhelper.InitBareRepo(t)

		cmd := exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			cleanup()
			t.Log(err)
			t.FailNow()
		}

		return New(testRepo, config.Config), testRepoPath, cleanup
	}

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	t.Run("invalid name", func(t *testing.T) {
		repo := New(nil, config.Config)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, git.ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		testRepo, _, cleanup := testhelper.InitBareRepo(t)
		defer cleanup()

		repo := New(testRepo, config.Config)
		var stderr bytes.Buffer
		err := repo.FetchRemote(ctx, "stub", FetchOpts{Stderr: &stderr})
		require.Error(t, err)
		require.Contains(t, stderr.String(), "'stub' does not appear to be a git repository")
	})

	t.Run("ok", func(t *testing.T) {
		repo, testRepoPath, cleanup := initBareWithRemote(t, "origin")
		defer cleanup()

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Stderr: &stderr}))

		require.Empty(t, stderr.String(), "it should not produce output as it is called with --quite flag by default")

		fetchHeadData, err := ioutil.ReadFile(filepath.Join(testRepoPath, "FETCH_HEAD"))
		require.NoError(t, err, "it should create FETCH_HEAD with info about fetch")

		fetchHead := string(fetchHeadData)
		require.Contains(t, fetchHead, "e56497bb5f03a90a51293fc6d516788730953899	not-for-merge	branch ''test''")
		require.Contains(t, fetchHead, "8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b	not-for-merge	tag 'v1.1.0'")

		sha, err := repo.ResolveRevision(ctx, git.Revision("refs/remotes/origin/master^{commit}"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312"), sha)
	})

	t.Run("with env", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := New(testRepo, config.Config)
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --quiet source --end-of-options")
	})

	t.Run("with globals", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := New(testRepo, config.Config)
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(
			ctx,
			"source",
			FetchOpts{
				Global: []git.GlobalOption{git.ConfigPair{Key: "fetch.prune", Value: "true"}},
			}),
		)

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with prune", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := New(testRepo, config.Config)

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Prune: true}))

		contains, err := repo.HasRevision(ctx, git.Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with no tags", func(t *testing.T) {
		repo, testRepoPath, cleanup := initBareWithRemote(t, "origin")
		defer cleanup()

		tagsBefore := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsBefore)

		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Tags: FetchOptsTagsNone, Force: true}))

		tagsAfter := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsAfter)

		containsBranches, err := repo.HasRevision(ctx, git.Revision("'test'"))
		require.NoError(t, err)
		require.False(t, containsBranches)

		containsTags, err := repo.HasRevision(ctx, git.Revision("v1.1.0"))
		require.NoError(t, err)
		require.False(t, containsTags)
	})
}
