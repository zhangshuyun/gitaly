package git

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os/exec"
	"path/filepath"
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
	require.Equal(t, LocalRepositoryRemote{repo: repository}, repo.Remote())
}

func TestLocalRepositoryRemote_Add(t *testing.T) {
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

	remote := LocalRepositoryRemote{repo: repo}

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

func TestLocalRepositoryRemote_Remove(t *testing.T) {
	repo, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	remote := LocalRepositoryRemote{repo: repo}

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

func TestLocalRepositoryRemote_SetURL(t *testing.T) {
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
				remote := LocalRepositoryRemote{repo: repo}
				err := remote.SetURL(ctx, tc.name, tc.url, SetURLOpts{})
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidArg))
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "add", "first", "file:/"+repoPath)

		remote := LocalRepositoryRemote{repo: repo}
		require.NoError(t, remote.SetURL(ctx, "first", "http://some.com.git", SetURLOpts{Push: true}))

		remotes := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "--verbose"))
		require.Equal(t,
			"first	file:/"+repoPath+" (fetch)\n"+
				"first	http://some.com.git (push)",
			remotes,
		)
	})

	t.Run("doesnt exist", func(t *testing.T) {
		remote := LocalRepositoryRemote{repo: repo}
		err := remote.SetURL(ctx, "second", "http://some.com.git", SetURLOpts{})
		require.True(t, errors.Is(err, ErrNotFound), err)
	})
}

func TestLocalRepository_FetchRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, remoteRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	initBareWithRemote := func(t *testing.T, remote string) (*LocalRepository, string, testhelper.Cleanup) {
		t.Helper()

		testRepo, testRepoPath, cleanup := testhelper.InitBareRepo(t)

		cmd := exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			cleanup()
			t.Log(err)
			t.FailNow()
		}

		return NewRepository(testRepo, config.Config), testRepoPath, cleanup
	}

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	t.Run("invalid name", func(t *testing.T) {
		repo := NewRepository(nil, config.Config)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		testRepo, _, cleanup := testhelper.InitBareRepo(t)
		defer cleanup()

		repo := NewRepository(testRepo, config.Config)
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

		sha, err := repo.ResolveRevision(ctx, Revision("refs/remotes/origin/master^{commit}"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, ObjectID("1e292f8fedd741b75372e19097c76d327140c312"), sha)
	})

	t.Run("with env", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := NewRepository(testRepo, config.Config)
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

		repo := NewRepository(testRepo, config.Config)
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(
			ctx,
			"source",
			FetchOpts{
				Global: []GlobalOption{ConfigPair{Key: "fetch.prune", Value: "true"}},
			}),
		)

		contains, err := repo.HasRevision(ctx, Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with prune", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := NewRepository(testRepo, config.Config)

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Prune: true}))

		contains, err := repo.HasRevision(ctx, Revision("refs/remotes/source/markdown"))
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

		containsBranches, err := repo.HasRevision(ctx, Revision("'test'"))
		require.NoError(t, err)
		require.False(t, containsBranches)

		containsTags, err := repo.HasRevision(ctx, Revision("v1.1.0"))
		require.NoError(t, err)
		require.False(t, containsTags)
	})
}
