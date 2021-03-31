package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_extractHostFromRemoteURL(t *testing.T) {
	testCases := []struct {
		raw       string
		canonical string
		invalid   bool
	}{
		{raw: "tcp://1.2.3.4", canonical: "1.2.3.4"},
		{raw: "tcp://1.2.3.4:567", canonical: "1.2.3.4:567"},
		{raw: "tcp://foobar", canonical: "foobar"},
		{raw: "tcp://foobar:567", canonical: "foobar:567"},
		{raw: "tcp://1.2.3.4/foo/bar.socket", invalid: true},
		{raw: "tls://1.2.3.4/foo/bar.socket", invalid: true},
		{raw: "tcp:///foo/bar.socket", invalid: true},
		{raw: "tcp:/foo/bar.socket", invalid: true},
		{raw: "tcp://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999", canonical: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999"},
		{raw: "foobar:9999", invalid: true},
		{raw: "unix:/foo/bar.socket", invalid: true},
		{raw: "unix:///foo/bar.socket", invalid: true},
		{raw: "unix://foo/bar.socket", invalid: true},
		{raw: "unix:foo/bar.socket", invalid: true},
	}

	for _, tc := range testCases {
		t.Run(tc.raw, func(t *testing.T) {
			canonical, err := extractHostFromRemoteURL(tc.raw)
			if tc.invalid {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.canonical, canonical)
		})
	}
}

func Test_extractPathFromSocketURL(t *testing.T) {
	testCases := []struct {
		raw     string
		path    string
		invalid bool
	}{
		{raw: "unix:/foo/bar.socket", path: "/foo/bar.socket"},
		{raw: "unix:///foo/bar.socket", path: "///foo/bar.socket"}, // Silly but valid
		{raw: "unix:foo/bar.socket", path: "foo/bar.socket"},
		{raw: "unix:../foo/bar.socket", path: "../foo/bar.socket"},
		{raw: "unix:path/with/a/colon:/in/it", path: "path/with/a/colon:/in/it"},
		{raw: "tcp://1.2.3.4", invalid: true},
		{raw: "foo/bar.socket", invalid: true},
	}

	for _, tc := range testCases {
		t.Run(tc.raw, func(t *testing.T) {
			path, err := extractPathFromSocketURL(tc.raw)

			if tc.invalid {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.path, path)
		})
	}
}
