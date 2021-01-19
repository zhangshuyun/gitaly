package git

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestLocalRepository_Remote(t *testing.T) {
	repository := &gitalypb.Repository{StorageName: "stub", RelativePath: "/stub"}

	repo := NewRepository(repository, config.Config)
	require.Equal(t, RepositoryRemote{repo: repository}, repo.Remote())
}

func TestAddRemoteOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts RemoteAddOpts
		exp  []Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: RemoteAddOpts{
				Tags:                   RemoteAddOptsTagsNone,
				Fetch:                  true,
				RemoteTrackingBranches: []string{"branch-1", "branch-2"},
				DefaultBranch:          "develop",
				Mirror:                 RemoteAddOptsMirrorPush,
			},
			exp: []Option{
				ValueFlag{Name: "-t", Value: "branch-1"},
				ValueFlag{Name: "-t", Value: "branch-2"},
				ValueFlag{Name: "-m", Value: "develop"},
				Flag{Name: "-f"},
				Flag{Name: "--no-tags"},
				ValueFlag{Name: "--mirror", Value: "push"},
			},
		},
		{
			desc: "with tags",
			opts: RemoteAddOpts{Tags: RemoteAddOptsTagsAll},
			exp:  []Option{Flag{Name: "--tags"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}

func TestRepositoryRemote_Add(t *testing.T) {
	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	_, remoteRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "remove", "origin")

	ctx, cancel := testhelper.Context()
	defer cancel()

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	remote := RepositoryRemote{repo: repo}

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
				err := remote.Add(ctx, tc.name, tc.url, RemoteAddOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("fetch", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "first", remoteRepoPath, RemoteAddOpts{Fetch: true}))

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
		require.NoError(t, remote.Add(ctx, "second", "http://some.com.git", RemoteAddOpts{DefaultBranch: "wip"}))

		defaultRemote := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "symbolic-ref", "refs/remotes/second/HEAD"))
		require.Equal(t, "refs/remotes/second/wip", defaultRemote)
	})

	t.Run("remote tracking branches", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "third", "http://some.com.git", RemoteAddOpts{RemoteTrackingBranches: []string{"a", "b"}}))

		defaultRemote := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--get-all", "remote.third.fetch"))
		require.Equal(t, "+refs/heads/a:refs/remotes/third/a\n+refs/heads/b:refs/remotes/third/b", defaultRemote)
	})

	t.Run("already exists", func(t *testing.T) {
		require.NoError(t, remote.Add(ctx, "fourth", "http://some.com.git", RemoteAddOpts{}))

		err := remote.Add(ctx, "fourth", "http://some.com.git", RemoteAddOpts{})
		require.Equal(t, ErrAlreadyExists, err)
	})
}

func TestRepositoryRemote_Remove(t *testing.T) {
	repo, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	remote := RepositoryRemote{repo: repo}

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "add", "first", "http://some.com.git")

		require.NoError(t, remote.Remove(ctx, "first"))

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
		require.Empty(t, remotes)
	})

	t.Run("not found", func(t *testing.T) {
		err := remote.Remove(ctx, "second")
		require.Equal(t, ErrNotFound, err)
	})

	t.Run("invalid argument: name", func(t *testing.T) {
		err := remote.Remove(ctx, " ")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidArg))
		assert.Contains(t, err.Error(), `"name" is blank or empty`)
	})
}

func TestRemoteSetURLOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts SetURLOpts
		exp  []Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: SetURLOpts{Push: true},
			exp:  []Option{Flag{Name: "--push"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}

func TestRepositoryRemote_SetURL(t *testing.T) {
	repo, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

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
				remote := RepositoryRemote{repo: repo}
				err := remote.SetURL(ctx, tc.name, tc.url, SetURLOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "add", "first", "file:/"+repoPath)

		remote := RepositoryRemote{repo: repo}
		require.NoError(t, remote.SetURL(ctx, "first", "http://some.com.git", SetURLOpts{Push: true}))

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	file:/"+repoPath+" (fetch)\n"+
				"first	http://some.com.git (push)",
			remotes,
		)
	})

	t.Run("doesnt exist", func(t *testing.T) {
		remote := RepositoryRemote{repo: repo}
		err := remote.SetURL(ctx, "second", "http://some.com.git", SetURLOpts{})
		require.True(t, errors.Is(err, ErrNotFound), err)
	})
}
