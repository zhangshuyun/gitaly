package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion_LessThan(t *testing.T) {
	for _, tc := range []struct {
		v1, v2 string
		expect bool
	}{
		// v1 < v2 == expect
		{"0.0.0", "0.0.0", false},
		{"0.0.0", "0.0.1", true},
		{"0.0.0", "0.1.0", true},
		{"0.0.0", "0.1.1", true},
		{"0.0.0", "1.0.0", true},
		{"0.0.0", "1.0.1", true},
		{"0.0.0", "1.1.0", true},
		{"0.0.0", "1.1.1", true},

		{"0.0.1", "0.0.0", false},
		{"0.0.1", "0.0.1", false},
		{"0.0.1", "0.1.0", true},
		{"0.0.1", "0.1.1", true},
		{"0.0.1", "1.0.0", true},
		{"0.0.1", "1.0.1", true},
		{"0.0.1", "1.1.0", true},
		{"0.0.1", "1.1.1", true},

		{"0.1.0", "0.0.0", false},
		{"0.1.0", "0.0.1", false},
		{"0.1.0", "0.1.0", false},
		{"0.1.0", "0.1.1", true},
		{"0.1.0", "1.0.0", true},
		{"0.1.0", "1.0.1", true},
		{"0.1.0", "1.1.0", true},
		{"0.1.0", "1.1.1", true},

		{"0.1.1", "0.0.0", false},
		{"0.1.1", "0.0.1", false},
		{"0.1.1", "0.1.0", false},
		{"0.1.1", "0.1.1", false},
		{"0.1.1", "1.0.0", true},
		{"0.1.1", "1.0.1", true},
		{"0.1.1", "1.1.0", true},
		{"0.1.1", "1.1.1", true},

		{"1.0.0", "0.0.0", false},
		{"1.0.0", "0.0.1", false},
		{"1.0.0", "0.1.0", false},
		{"1.0.0", "0.1.1", false},
		{"1.0.0", "1.0.0", false},
		{"1.0.0", "1.0.1", true},
		{"1.0.0", "1.1.0", true},
		{"1.0.0", "1.1.1", true},

		{"1.0.1", "0.0.0", false},
		{"1.0.1", "0.0.1", false},
		{"1.0.1", "0.1.0", false},
		{"1.0.1", "0.1.1", false},
		{"1.0.1", "1.0.0", false},
		{"1.0.1", "1.0.1", false},
		{"1.0.1", "1.1.0", true},
		{"1.0.1", "1.1.1", true},

		{"1.1.0", "0.0.0", false},
		{"1.1.0", "0.0.1", false},
		{"1.1.0", "0.1.0", false},
		{"1.1.0", "0.1.1", false},
		{"1.1.0", "1.0.0", false},
		{"1.1.0", "1.0.1", false},
		{"1.1.0", "1.1.0", false},
		{"1.1.0", "1.1.1", true},

		{"1.1.1", "0.0.0", false},
		{"1.1.1", "0.0.1", false},
		{"1.1.1", "0.1.0", false},
		{"1.1.1", "0.1.1", false},
		{"1.1.1", "1.0.0", false},
		{"1.1.1", "1.0.1", false},
		{"1.1.1", "1.1.0", false},
		{"1.1.1", "1.1.1", false},

		{"1.1.1.rc0", "1.1.1", true},
		{"1.1.1.rc0", "1.1.1.rc0", false},
		{"1.1.1.rc0", "1.1.0", false},
		{"1.1.1-rc0", "1.1.1-rc0", false},
		{"1.1.1-rc0", "1.1.1", true},
		{"1.1.1", "1.1.1-rc0", false},

		{"1.1.GIT", "1.1.1", true},
		{"1.1.GIT", "1.1.0", false},
		{"1.1.GIT", "1.0.0", false},
	} {
		t.Run(fmt.Sprintf("%s < %s", tc.v1, tc.v2), func(t *testing.T) {
			v1, err := parseVersion(tc.v1)
			require.NoError(t, err)

			v2, err := parseVersion(tc.v2)
			require.NoError(t, err)

			require.Equal(t, tc.expect, v1.LessThan(v2))
		})
	}
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
		{"2.29.0-rc0", false},
		{"2.29.0", true},
		{"2.29.1", true},
		{"3.0.0", true},
	} {
		version, err := parseVersion(tc.version)
		require.NoError(t, err)
		require.Equal(t, tc.expect, version.IsSupported())
	}
}

func TestVersion_SupportsConfigEnv(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.20.0", false},
		{"2.24.0-rc0", false},
		{"2.24.0", false},
		{"2.25.0", false},
		{"2.31.0", true},
		{"2.31.1", true},
		{"3.0.0", true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			version, err := parseVersion(tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.expect, version.SupportsConfigEnv())
		})
	}
}

func TestVersion_SupportsAtomicFetches(t *testing.T) {
	for _, tc := range []struct {
		version string
		expect  bool
	}{
		{"2.25.0", false},
		{"2.30.1", false},
		{"2.31.0-rc0", false},
		{"2.31.0", true},
		{"2.31.1", true},
		{"3.0.0", true},
	} {
		version, err := parseVersion(tc.version)
		require.NoError(t, err)
		require.Equal(t, tc.expect, version.SupportsAtomicFetches())
	}
}
