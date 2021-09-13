package git

import (
	"context"
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

type versionGitCommandFactory struct {
	CommandFactory

	t       *testing.T
	version string
}

func newVersionGitCommandFactory(t *testing.T, version string) *versionGitCommandFactory {
	return &versionGitCommandFactory{
		t:       t,
		version: version,
	}
}

func (f *versionGitCommandFactory) NewWithoutRepo(ctx context.Context, subcmd Cmd, opts ...CmdOpt) (*command.Command, error) {
	f.t.Helper()

	require.Equal(f.t, SubCmd{
		Name: "version",
	}, subcmd)
	require.Len(f.t, opts, 0)

	cmd, err := command.New(ctx, exec.Command("/usr/bin/env", "echo", f.version), nil, nil, nil)
	require.NoError(f.t, err)

	return cmd, nil
}

type versionRepositoryExecutor struct {
	RepositoryExecutor

	t       *testing.T
	version string
}

func newVersionRepositoryExecutor(t *testing.T, version string) *versionRepositoryExecutor {
	return &versionRepositoryExecutor{
		t:       t,
		version: version,
	}
}

func (e *versionRepositoryExecutor) Exec(ctx context.Context, subcmd Cmd, opts ...CmdOpt) (*command.Command, error) {
	e.t.Helper()

	require.Equal(e.t, SubCmd{
		Name: "version",
	}, subcmd)
	require.Len(e.t, opts, 0)

	cmd, err := command.New(ctx, exec.Command("/usr/bin/env", "echo", e.version), nil, nil, nil)
	require.NoError(e.t, err)

	return cmd, nil
}

func TestCurrentVersion(t *testing.T) {
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
			t.Run("command factory", func(t *testing.T) {
				actualVersion, err := CurrentVersion(ctx, newVersionGitCommandFactory(t, tc.versionString))
				if tc.expectedErr == "" {
					require.NoError(t, err)
				} else {
					require.EqualError(t, err, tc.expectedErr)
				}
				require.Equal(t, tc.expectedVersion, actualVersion)
			})

			t.Run("repository executor", func(t *testing.T) {
				actualVersion, err := CurrentVersionForExecutor(ctx, newVersionRepositoryExecutor(t, tc.versionString))
				if tc.expectedErr == "" {
					require.NoError(t, err)
				} else {
					require.EqualError(t, err, tc.expectedErr)
				}
				require.Equal(t, tc.expectedVersion, actualVersion)
			})
		})
	}
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
		{"2.30.0", false},
		{"2.31.0-rc0", false},
		{"2.31.0", true},
		{"2.31.0.gl0", true},
		{"2.31.0.gl1", true},
		{"2.31.1", true},
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

func TestVersion_SupportsObjectTypeFilter(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.32.0.gl3", false},
		{"2.33.0.gl3", true},
		{"2.33.1.gl3", true},
		{"2.34.0", true},
		{"3.0.0", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.FlushesUpdaterefStatus())
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
		{"2.32.0", true},
		{"2.32.0.gl0", true},
		{"2.32.0.gl1", true},
		{"2.32.1", true},
		{"3.0.0", true},
		{"3.0.0.gl5", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.SupportsObjectTypeFilter())
		})
	}
}
