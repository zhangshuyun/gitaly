package localrepo

import (
	"errors"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestRepo_Config(t *testing.T) {
	bareRepo, _, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	repo := New(nil, bareRepo, config.Cfg{})
	require.Equal(t, Config{repo: repo}, repo.Config())
}

func TestBuildConfigAddOptsFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts git.ConfigAddOpts
		exp  []git.Option
	}{
		{
			desc: "none",
			opts: git.ConfigAddOpts{},
			exp:  nil,
		},
		{
			desc: "all set",
			opts: git.ConfigAddOpts{Type: git.ConfigTypeBoolOrInt},
			exp:  []git.Option{git.Flag{Name: "--bool-or-int"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, buildConfigAddOptsFlags(tc.opts))
		})
	}
}

func TestConfig_Add(t *testing.T) {
	repoProto, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()
	repo := New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

	config := repo.Config()

	t.Run("ok", func(t *testing.T) {
		require.NoError(t, config.Add(ctx, "key.one", "1", git.ConfigAddOpts{}))

		actual := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "key.one"))
		require.Equal(t, "1", actual)
	})

	t.Run("appends to an old value", func(t *testing.T) {
		require.NoError(t, config.Add(ctx, "key.two", "2", git.ConfigAddOpts{}))
		require.NoError(t, config.Add(ctx, "key.two", "3", git.ConfigAddOpts{}))

		actual := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--get-all", "key.two"))
		require.Equal(t, "2\n3", actual)
	})

	t.Run("options are passed", func(t *testing.T) {
		require.NoError(t, config.Add(ctx, "key.three", "3", git.ConfigAddOpts{Type: git.ConfigTypeInt}))

		actual := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--int", "key.three"))
		require.Equal(t, "3", actual)
	})

	t.Run("invalid argument", func(t *testing.T) {
		for _, tc := range []struct {
			desc   string
			name   string
			expErr error
			expMsg string
		}{
			{
				desc:   "empty name",
				name:   "",
				expErr: git.ErrInvalidArg,
				expMsg: `"name" is blank or empty`,
			},
			{
				desc:   "invalid name",
				name:   "`.\n",
				expErr: git.ErrInvalidArg,
				expMsg: "bad section or name",
			},
			{
				desc:   "no section or name",
				name:   "missing",
				expErr: git.ErrInvalidArg,
				expMsg: "missing section or name",
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				ctx, cancel := testhelper.Context()
				defer cancel()

				config := repo.Config()
				err := config.Add(ctx, tc.name, "some", git.ConfigAddOpts{})
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expErr), err.Error())
				require.Contains(t, err.Error(), tc.expMsg)
			})
		}
	})
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
	repoProto, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()
	repo := New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

	config := repo.Config()

	t.Run("ok", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.one", "one")
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.two", "2")
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.three", "!@#$%^&")

		vals, err := config.GetRegexp(ctx, "^key\\..*o", git.ConfigGetRegexpOpts{})
		require.NoError(t, err)
		require.Equal(t, []git.ConfigPair{{Key: "key.one", Value: "one"}, {Key: "key.two", Value: "2"}}, vals)
	})

	t.Run("show origin and scope", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.four", "4")
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.five", "five")

		exp := []git.ConfigPair{
			{Key: "key.four", Value: "4", Origin: "file:" + filepath.Join(repoPath, "config"), Scope: "local"},
			{Key: "key.five", Value: "five", Origin: "file:" + filepath.Join(repoPath, "config"), Scope: "local"},
		}

		vals, err := config.GetRegexp(ctx, "^key\\.f", git.ConfigGetRegexpOpts{ShowScope: true, ShowOrigin: true})
		require.NoError(t, err)
		require.Equal(t, exp, vals)
	})

	t.Run("none found", func(t *testing.T) {
		vals, err := config.GetRegexp(ctx, "nonexisting", git.ConfigGetRegexpOpts{})
		require.NoError(t, err)
		require.Empty(t, vals)
	})

	t.Run("bad combination of regexp and type", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.six", "key-six")

		_, err := config.GetRegexp(ctx, "^key\\.six$", git.ConfigGetRegexpOpts{Type: git.ConfigTypeBool})
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
				_, err := config.GetRegexp(ctx, tc.regexp, git.ConfigGetRegexpOpts{})
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
		data, err := ioutil.ReadFile(filepath.Join(repoPath, "config"))
		require.NoError(t, err)
		require.Contains(t, string(data), "[core]", "config should have core section defined by default")
		return func(t *testing.T, val string, contains bool) {
			require.Equal(t, contains, strings.Contains(string(data), val))
		}
	}

	repoProto, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()
	repo := New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

	config := repo.Config()

	t.Run("unset single value", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.one", "key-one")

		require.NoError(t, config.Unset(ctx, "key.one", git.ConfigUnsetOpts{}))

		contains := configContains(t, repoPath)
		contains(t, "key-one", false)
	})

	t.Run("unset multiple values", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.two", "key-two-1")
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.two", "key-two-2")

		require.NoError(t, config.Unset(ctx, "key.two", git.ConfigUnsetOpts{All: true}))

		contains := configContains(t, repoPath)
		contains(t, "key-two-1", false)
		contains(t, "key-two-2", false)
	})

	t.Run("unset single with multiple values", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.two", "key-two-1")
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.two", "key-two-2")

		err := config.Unset(ctx, "key.two", git.ConfigUnsetOpts{})
		require.Equal(t, git.ErrNotFound, err)

		contains := configContains(t, repoPath)
		contains(t, "key-two-1", true)
		contains(t, "key-two-2", true)
	})

	t.Run("config key doesn't exist - is strict (by default)", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.three", "key-three")

		err := config.Unset(ctx, "some.stub", git.ConfigUnsetOpts{})
		require.Equal(t, git.ErrNotFound, err)

		contains := configContains(t, repoPath)
		contains(t, "key-three", true)
	})

	t.Run("config key doesn't exist - not strict", func(t *testing.T) {
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--add", "key.four", "key-four")

		require.NoError(t, config.Unset(ctx, "some.stub", git.ConfigUnsetOpts{NotStrict: true}))

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
				err := config.Unset(ctx, tc.name, git.ConfigUnsetOpts{})
				require.Error(t, err)
				require.True(t, errors.Is(err, tc.expErr), err.Error())
			})
		}
	})
}
