package localrepo

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"google.golang.org/grpc/peer"
)

func setupRepoConfig(t *testing.T) (Config, string) {
	t.Helper()

	cfg := testcfg.Build(t)

	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	repo := New(gitCmdFactory, catfile.NewCache(cfg), repoProto, cfg)

	return repo.Config(), repoPath
}

func TestBuildConfigGetRegexpOptsFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts git.ConfigGetRegexpOpts
		exp  []git.Option
	}{
		{
			desc: "none",
			opts: git.ConfigGetRegexpOpts{},
			exp:  nil,
		},
		{
			desc: "all set",
			opts: git.ConfigGetRegexpOpts{Type: git.ConfigTypeInt, ShowOrigin: true, ShowScope: true},
			exp: []git.Option{
				git.Flag{Name: "--int"},
				git.Flag{Name: "--show-origin"},
				git.Flag{Name: "--show-scope"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, buildConfigGetRegexpOptsFlags(tc.opts))
		})
	}
}

func TestConfig_GetRegexp(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repoConfig, repoPath := setupRepoConfig(t)

	t.Run("ok", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.one", "one")
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.two", "2")
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.three", "!@#$%^&")

		vals, err := repoConfig.GetRegexp(ctx, "^key\\..*o", git.ConfigGetRegexpOpts{})
		require.NoError(t, err)
		require.Equal(t, []git.ConfigPair{{Key: "key.one", Value: "one"}, {Key: "key.two", Value: "2"}}, vals)
	})

	t.Run("show origin and scope", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.four", "4")
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.five", "five")

		exp := []git.ConfigPair{
			{Key: "key.four", Value: "4", Origin: "file:" + filepath.Join(repoPath, "config"), Scope: "local"},
			{Key: "key.five", Value: "five", Origin: "file:" + filepath.Join(repoPath, "config"), Scope: "local"},
		}

		vals, err := repoConfig.GetRegexp(ctx, "^key\\.f", git.ConfigGetRegexpOpts{ShowScope: true, ShowOrigin: true})
		require.NoError(t, err)
		require.Equal(t, exp, vals)
	})

	t.Run("none found", func(t *testing.T) {
		vals, err := repoConfig.GetRegexp(ctx, "nonexisting", git.ConfigGetRegexpOpts{})
		require.NoError(t, err)
		require.Empty(t, vals)
	})

	t.Run("bad combination of regexp and type", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.six", "key-six")

		_, err := repoConfig.GetRegexp(ctx, "^key\\.six$", git.ConfigGetRegexpOpts{Type: git.ConfigTypeBool})
		require.Error(t, err)
		require.True(t, errors.Is(err, git.ErrInvalidArg))
		require.Contains(t, err.Error(), "fetched result doesn't correspond to requested type")
	})

	t.Run("invalid argument", func(t *testing.T) {
		for _, tc := range []struct {
			desc   string
			regexp string
			expErr error
			expMsg string
		}{
			{
				desc:   "empty regexp",
				regexp: "",
				expErr: git.ErrInvalidArg,
				expMsg: `"nameRegexp" is blank or empty`,
			},
			{
				desc:   "invalid regexp",
				regexp: "{4",
				expErr: git.ErrInvalidArg,
				expMsg: "regexp has a bad format",
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				_, err := repoConfig.GetRegexp(ctx, tc.regexp, git.ConfigGetRegexpOpts{})
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expErr), err.Error())
				require.Contains(t, err.Error(), tc.expMsg)
			})
		}
	})
}

func TestBuildConfigUnsetOptsFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts git.ConfigUnsetOpts
		exp  []git.Option
	}{
		{
			desc: "none",
			opts: git.ConfigUnsetOpts{},
			exp:  []git.Option{git.Flag{Name: "--unset"}},
		},
		{
			desc: "all set",
			opts: git.ConfigUnsetOpts{All: true, NotStrict: true},
			exp:  []git.Option{git.Flag{Name: "--unset-all"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, buildConfigUnsetOptsFlags(tc.opts))
		})
	}
}

func TestConfig_UnsetAll(t *testing.T) {
	configContains := func(t *testing.T, repoPath string) func(t *testing.T, val string, contains bool) {
		data := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
		require.Contains(t, string(data), "[core]", "config should have core section defined by default")
		return func(t *testing.T, val string, contains bool) {
			require.Equal(t, contains, strings.Contains(string(data), val))
		}
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoConfig, repoPath := setupRepoConfig(t)

	t.Run("unset single value", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.one", "key-one")

		require.NoError(t, repoConfig.Unset(ctx, "key.one", git.ConfigUnsetOpts{}))

		contains := configContains(t, repoPath)
		contains(t, "key-one", false)
	})

	t.Run("unset multiple values", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.two", "key-two-1")
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.two", "key-two-2")

		require.NoError(t, repoConfig.Unset(ctx, "key.two", git.ConfigUnsetOpts{All: true}))

		contains := configContains(t, repoPath)
		contains(t, "key-two-1", false)
		contains(t, "key-two-2", false)
	})

	t.Run("unset single with multiple values", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.two", "key-two-1")
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.two", "key-two-2")

		err := repoConfig.Unset(ctx, "key.two", git.ConfigUnsetOpts{})
		require.Equal(t, git.ErrNotFound, err)

		contains := configContains(t, repoPath)
		contains(t, "key-two-1", true)
		contains(t, "key-two-2", true)
	})

	t.Run("config key doesn't exist - is strict (by default)", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.three", "key-three")

		err := repoConfig.Unset(ctx, "some.stub", git.ConfigUnsetOpts{})
		require.Equal(t, git.ErrNotFound, err)

		contains := configContains(t, repoPath)
		contains(t, "key-three", true)
	})

	t.Run("config key doesn't exist - not strict", func(t *testing.T) {
		gittest.Exec(t, repoConfig.repo.cfg, "-C", repoPath, "config", "--add", "key.four", "key-four")

		require.NoError(t, repoConfig.Unset(ctx, "some.stub", git.ConfigUnsetOpts{NotStrict: true}))

		contains := configContains(t, repoPath)
		contains(t, "key-four", true)
	})

	t.Run("invalid argument", func(t *testing.T) {
		for _, tc := range []struct {
			desc   string
			name   string
			expErr error
		}{
			{
				desc:   "empty name",
				name:   "",
				expErr: git.ErrInvalidArg,
			},
			{
				desc:   "invalid name",
				name:   "`.\n",
				expErr: git.ErrInvalidArg,
			},
			{
				desc:   "no section or name",
				name:   "bad",
				expErr: git.ErrInvalidArg,
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				err := repoConfig.Unset(ctx, tc.name, git.ConfigUnsetOpts{})
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expErr), err.Error())
			})
		}
	})
}

func TestRepo_SetConfig(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)

	standardEntries := []string{
		"core.repositoryformatversion=0",
		"core.filemode=true",
		"core.bare=true",
	}

	type configEntry struct {
		key, value string
	}

	for _, tc := range []struct {
		desc               string
		preexistingEntries []configEntry
		key                string
		value              string
		locked             bool
		expectedEntries    []string
		expectedErr        error
	}{
		{
			desc:            "simple addition",
			key:             "my.key",
			value:           "value",
			expectedEntries: append(standardEntries, "my.key=value"),
		},
		{
			desc: "overwrite preexisting value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: append(standardEntries, "preexisting.key=overridden"),
		},
		{
			desc: "overwrite multi-value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value-1"},
				{"preexisting.key", "value-2"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: append(standardEntries, "preexisting.key=overridden"),
		},
		{
			desc:            "invalid key",
			key:             "missingsection",
			value:           "overridden",
			expectedEntries: standardEntries,
			expectedErr:     fmt.Errorf("%w: missing section or name", git.ErrInvalidArg),
		},
		{
			desc:            "locked",
			key:             "my.key",
			value:           "value",
			locked:          true,
			expectedEntries: standardEntries,
			expectedErr:     fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", errors.New("file already locked"))),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			repo := NewTestRepo(t, cfg, repoProto)

			for _, entry := range tc.preexistingEntries {
				gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", entry.key, entry.value)
			}

			if tc.locked {
				writer, err := safe.NewLockingFileWriter(filepath.Join(repoPath, "config"))
				require.NoError(t, err)
				defer func() { require.NoError(t, writer.Close()) }()
				require.NoError(t, writer.Lock())
			}

			require.Equal(t, tc.expectedErr, repo.SetConfig(ctx, tc.key, tc.value, &transaction.MockManager{}))

			output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
			require.Equal(t, tc.expectedEntries, strings.Split(text.ChompBytes(output), "\n"))
		})
	}

	t.Run("transactional", func(t *testing.T) {
		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := NewTestRepo(t, cfg, repoProto)

		backchannelPeer := &peer.Peer{
			AuthInfo: backchannel.WithID(nil, 1234),
		}

		ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
		ctx = peer.NewContext(ctx, backchannelPeer)

		votes := 0

		require.NoError(t, err)
		require.NoError(t, repo.SetConfig(ctx, "some.key", "value", &transaction.MockManager{
			VoteFn: func(context.Context, txinfo.Transaction, voting.Vote) error {
				votes++
				return nil
			},
		}))

		require.Equal(t, 2, votes)
	})
}

func TestRepo_UnsetMatchingConfig(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)

	standardKeys := []string{
		"core.repositoryformatversion",
		"core.filemode",
		"core.bare",
	}

	for _, tc := range []struct {
		desc         string
		addEntries   map[string]string
		regex        string
		locked       bool
		expectedErr  error
		expectedKeys []string
	}{
		{
			desc:         "empty regex is refused",
			regex:        "",
			expectedErr:  fmt.Errorf("%w: \"regex\" is blank or empty", git.ErrInvalidArg),
			expectedKeys: standardKeys,
		},
		{
			desc: "simple match",
			addEntries: map[string]string{
				"foo.bar": "value1",
				"foo.qux": "value2",
			},
			regex:        "foo.bar",
			expectedKeys: append(standardKeys, "foo.qux"),
		},
		{
			desc: "multiple matches",
			addEntries: map[string]string{
				"foo.bar": "value1",
				"foo.qux": "value2",
			},
			regex:        "foo.",
			expectedKeys: standardKeys,
		},
		{
			desc: "unanchored",
			addEntries: map[string]string{
				"foo.matchme": "value1",
				"foo.qux":     "value2",
			},
			regex:        "matchme",
			expectedKeys: append(standardKeys, "foo.qux"),
		},
		{
			desc: "anchored",
			addEntries: map[string]string{
				"foo.matchme": "value1",
				"matchme.foo": "value2",
			},
			regex:        "^matchme",
			expectedKeys: append(standardKeys, "foo.matchme"),
		},
		{
			desc:         "no matches",
			regex:        "idontmatch",
			expectedErr:  fmt.Errorf("%w: no matching keys", git.ErrNotFound),
			expectedKeys: standardKeys,
		},
		{
			desc:         "invalid regex",
			regex:        "?",
			expectedErr:  fmt.Errorf("%w: invalid regular expression", git.ErrInvalidArg),
			expectedKeys: standardKeys,
		},
		{
			desc:         "locked",
			regex:        ".*",
			locked:       true,
			expectedErr:  fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", errors.New("file already locked"))),
			expectedKeys: standardKeys,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
			repo := NewTestRepo(t, cfg, repoProto)

			for key, value := range tc.addEntries {
				gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", key, value)
			}

			if tc.locked {
				writer, err := safe.NewLockingFileWriter(filepath.Join(repoPath, "config"))
				require.NoError(t, err)
				defer func() { require.NoError(t, writer.Close()) }()
				require.NoError(t, writer.Lock())
			}

			require.Equal(t, tc.expectedErr, repo.UnsetMatchingConfig(ctx, tc.regex, &transaction.MockManager{}))

			output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--name-only", "--local")
			require.Equal(t, tc.expectedKeys, strings.Split(text.ChompBytes(output), "\n"))
		})
	}

	t.Run("transactional", func(t *testing.T) {
		repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := NewTestRepo(t, cfg, repoProto)

		gittest.Exec(t, cfg, "-C", repoPath, "config", "--add", "some.key", "value")

		backchannelPeer := &peer.Peer{
			AuthInfo: backchannel.WithID(nil, 1234),
		}

		ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
		ctx = peer.NewContext(ctx, backchannelPeer)

		votes := 0

		require.NoError(t, err)
		require.NoError(t, repo.UnsetMatchingConfig(ctx, "some.key", &transaction.MockManager{
			VoteFn: func(context.Context, txinfo.Transaction, voting.Vote) error {
				votes++
				return nil
			},
		}))

		require.Equal(t, 2, votes)
	})
}
