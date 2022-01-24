package localrepo

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

func TestRepo_SetConfig(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

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
			expectedEntries: []string{"my.key=value"},
		},
		{
			desc: "overwrite preexisting value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: []string{"preexisting.key=overridden"},
		},
		{
			desc: "overwrite multi-value",
			preexistingEntries: []configEntry{
				{"preexisting.key", "value-1"},
				{"preexisting.key", "value-2"},
			},
			key:             "preexisting.key",
			value:           "overridden",
			expectedEntries: []string{"preexisting.key=overridden"},
		},
		{
			desc:        "invalid key",
			key:         "missingsection",
			value:       "overridden",
			expectedErr: fmt.Errorf("%w: missing section or name", git.ErrInvalidArg),
		},
		{
			desc:        "locked",
			key:         "my.key",
			value:       "value",
			locked:      true,
			expectedErr: fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", safe.ErrFileAlreadyLocked)),
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

			standardEntries := []string{
				"core.repositoryformatversion=0",
				"core.filemode=true",
				"core.bare=true",
			}

			if runtime.GOOS == "darwin" {
				standardEntries = append(standardEntries,
					"core.ignorecase=true",
					"core.precomposeunicode=true",
				)
			}

			output := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local")
			require.Equal(t,
				append(standardEntries, tc.expectedEntries...),
				strings.Split(text.ChompBytes(output), "\n"),
			)
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

		txManager := transaction.NewTrackingManager()

		require.NoError(t, err)
		require.NoError(t, repo.SetConfig(ctx, "some.key", "value", txManager))

		require.Equal(t, 2, len(txManager.Votes()))
	})
}

func TestRepo_UnsetMatchingConfig(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	standardKeys := []string{
		"core.repositoryformatversion",
		"core.filemode",
		"core.bare",
	}

	if runtime.GOOS == "darwin" {
		standardKeys = []string{
			"core.repositoryformatversion",
			"core.filemode",
			"core.bare",
			"core.ignorecase",
			"core.precomposeunicode",
		}
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
			expectedErr:  fmt.Errorf("committing config: %w", fmt.Errorf("locking file: %w", safe.ErrFileAlreadyLocked)),
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

		txManager := transaction.NewTrackingManager()

		require.NoError(t, err)
		require.NoError(t, repo.UnsetMatchingConfig(ctx, "some.key", txManager))

		require.Equal(t, 2, len(txManager.Votes()))
	})
}
