package git

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestExecCommandFactory_GitVersion(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc            string
		versionString   string
		expectedErr     string
		expectedVersion Version
	}{
		{
			desc:          "valid version",
			versionString: "git version 2.33.1.gl1",
			expectedVersion: Version{
				versionString: "2.33.1.gl1", major: 2, minor: 33, patch: 1, gl: 1,
			},
		},
		{
			desc:          "valid version with trailing newline",
			versionString: "git version 2.33.1.gl1\n",
			expectedVersion: Version{
				versionString: "2.33.1.gl1", major: 2, minor: 33, patch: 1, gl: 1,
			},
		},
		{
			desc:          "multi-line version",
			versionString: "git version 2.33.1.gl1\nfoobar\n",
			expectedErr:   "cannot parse git version: strconv.ParseUint: parsing \"1\\nfoobar\": invalid syntax",
		},
		{
			desc:          "unexpected format",
			versionString: "2.33.1\n",
			expectedErr:   "invalid version format: \"2.33.1\\n\\n\"",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			gitPath := filepath.Join(testhelper.TempDir(t), "git")
			testhelper.WriteExecutable(t, gitPath, []byte(fmt.Sprintf(
				`#!/usr/bin/env bash
				echo '%s'
			`, tc.versionString)))

			gitCmdFactory, cleanup, err := NewExecCommandFactory(config.Cfg{
				Git: config.Git{
					BinPath: gitPath,
				},
			}, WithSkipHooks())
			require.NoError(t, err)
			defer cleanup()

			actualVersion, err := gitCmdFactory.GitVersion(ctx)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
			}
			require.Equal(t, tc.expectedVersion, actualVersion)
		})
	}

	t.Run("caching", func(t *testing.T) {
		gitPath := filepath.Join(testhelper.TempDir(t), "git")
		testhelper.WriteExecutable(t, gitPath, []byte(
			`#!/usr/bin/env bash
			echo 'git version 1.2.3'
		`))

		stat, err := os.Stat(gitPath)
		require.NoError(t, err)

		gitCmdFactory, cleanup, err := NewExecCommandFactory(config.Cfg{
			Git: config.Git{
				BinPath: gitPath,
			},
		}, WithSkipHooks())
		require.NoError(t, err)
		defer cleanup()

		version, err := gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())
		require.Equal(t, stat, *gitCmdFactory.cachedGitStat)

		// We rewrite the file with the same content length and modification time such that
		// its file information doesn't change. As a result, information returned by
		// stat(3P) shouldn't differ and we should continue to see the cached version. This
		// is a known insufficiency, but it is extremely unlikely to ever happen in
		// production when the real Git binary changes.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(
			`#!/usr/bin/env bash
			echo 'git version 9.8.7'
		`))
		require.NoError(t, os.Chtimes(gitPath, stat.ModTime(), stat.ModTime()))

		// Given that we continue to use the cached version we shouldn't see any
		// change here.
		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "1.2.3", version.String())
		require.Equal(t, stat, *gitCmdFactory.cachedGitStat)

		// If we really replace the Git binary with something else, then we should
		// see a changed version.
		require.NoError(t, os.Remove(gitPath))
		testhelper.WriteExecutable(t, gitPath, []byte(
			`#!/usr/bin/env bash
			echo 'git version 2.34.1'
		`))

		stat, err = os.Stat(gitPath)
		require.NoError(t, err)

		version, err = gitCmdFactory.GitVersion(ctx)
		require.NoError(t, err)
		require.Equal(t, "2.34.1", version.String())
		require.Equal(t, stat, *gitCmdFactory.cachedGitStat)
	})
}

func TestVersion_LessThan(t *testing.T) {
	for _, tc := range []struct {
		smaller, larger string
	}{
		{"0.0.0", "0.0.0"},
		{"0.0.0", "0.0.1"},
		{"0.0.0", "0.1.0"},
		{"0.0.0", "0.1.1"},
		{"0.0.0", "1.0.0"},
		{"0.0.0", "1.0.1"},
		{"0.0.0", "1.1.0"},
		{"0.0.0", "1.1.1"},

		{"0.0.1", "0.0.1"},
		{"0.0.1", "0.1.0"},
		{"0.0.1", "0.1.1"},
		{"0.0.1", "1.0.0"},
		{"0.0.1", "1.0.1"},
		{"0.0.1", "1.1.0"},
		{"0.0.1", "1.1.1"},

		{"0.1.0", "0.1.0"},
		{"0.1.0", "0.1.1"},
		{"0.1.0", "1.0.0"},
		{"0.1.0", "1.0.1"},
		{"0.1.0", "1.1.0"},
		{"0.1.0", "1.1.1"},

		{"0.1.1", "0.1.1"},
		{"0.1.1", "1.0.0"},
		{"0.1.1", "1.0.1"},
		{"0.1.1", "1.1.0"},
		{"0.1.1", "1.1.1"},

		{"1.0.0", "1.0.0"},
		{"1.0.0", "1.0.1"},
		{"1.0.0", "1.1.0"},
		{"1.0.0", "1.1.1"},

		{"1.0.1", "1.0.1"},
		{"1.0.1", "1.1.0"},
		{"1.0.1", "1.1.1"},

		{"1.1.0", "1.1.0"},
		{"1.1.0", "1.1.1"},

		{"1.1.1", "1.1.1"},

		{"1.1.1.rc0", "1.1.1.rc0"},
		{"1.1.1.rc0", "1.1.1"},
		{"1.1.0", "1.1.1.rc0"},

		{"1.1.GIT", "1.1.1"},
		{"1.0.0", "1.1.GIT"},

		{"1.1.1", "1.1.1.gl1"},
		{"1.1.1.gl0", "1.1.1.gl1"},
		{"1.1.1.gl1", "1.1.1.gl2"},
		{"1.1.1.gl1", "1.1.2"},
	} {
		t.Run(fmt.Sprintf("%s < %s", tc.smaller, tc.larger), func(t *testing.T) {
			smaller, err := parseVersion(tc.smaller)
			require.NoError(t, err)

			larger, err := parseVersion(tc.larger)
			require.NoError(t, err)

			if tc.smaller == tc.larger {
				require.False(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			} else {
				require.True(t, smaller.LessThan(larger))
				require.False(t, larger.LessThan(smaller))
			}
		})
	}

	t.Run("1.1.GIT == 1.1.0", func(t *testing.T) {
		first, err := parseVersion("1.1.GIT")
		require.NoError(t, err)

		second, err := parseVersion("1.1.0")
		require.NoError(t, err)

		// This is a special case: "GIT" is treated the same as "0".
		require.False(t, first.LessThan(second))
		require.False(t, second.LessThan(first))
	})
}

func TestVersion_IsSupported(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.20.0", false},
		{"2.24.0-rc0", false},
		{"2.24.0", false},
		{"2.25.0", false},
		{"2.32.0", false},
		{"2.33.0-rc0", false},
		{"2.33.0", true},
		{"2.33.0.gl0", true},
		{"2.33.0.gl1", true},
		{"2.33.1", true},
		{"3.0.0", true},
		{"3.0.0.gl5", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.IsSupported())
		})
	}
}

func TestVersion_FlushesUpdaterefStatus(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.31.0", false},
		{"2.31.0-rc0", false},
		{"2.31.1", false},
		{"2.33.0", false},
		{"2.33.0.gl0", false},
		{"2.33.0.gl2", false},
		{"2.33.0.gl3", true},
		{"2.33.0.gl4", true},
		{"2.33.1", true},
		{"3.0.0", true},
		{"3.0.0.gl5", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.FlushesUpdaterefStatus())
		})
	}
}
