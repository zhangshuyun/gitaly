package client

import (
	"testing"
)

func TestParseAddress(t *testing.T) {
	testCases := []struct {
		raw     string
		network string
		addr    string
		invalid bool
	}{
		{raw: "unix:/foo/bar.socket", network: "unix", addr: "/foo/bar.socket"},
		{raw: "unix:///foo/bar.socket", network: "unix", addr: "/foo/bar.socket"},
		// Mainly for test purposes we explicitly want to support relative paths
		{raw: "unix://foo/bar.socket", network: "unix", addr: "foo/bar.socket"},
		{raw: "unix:foo/bar.socket", network: "unix", addr: "foo/bar.socket"},
		{raw: "tcp://1.2.3.4", network: "tcp", addr: "1.2.3.4"},
		{raw: "tcp://1.2.3.4:567", network: "tcp", addr: "1.2.3.4:567"},
		{raw: "tcp://foobar", network: "tcp", addr: "foobar"},
		{raw: "tcp://foobar:567", network: "tcp", addr: "foobar:567"},
		{raw: "tcp://1.2.3.4/foo/bar.socket", invalid: true},
		{raw: "tcp:///foo/bar.socket", invalid: true},
		{raw: "tcp:/foo/bar.socket", invalid: true},
	}

	for _, tc := range testCases {
		network, addr, err := parseAddress(tc.raw)

		if err == nil && tc.invalid {
			t.Errorf("%v: expected error, got none", tc)
		} else if err != nil && !tc.invalid {
			t.Errorf("%v: parse error: %v", tc, err)
			continue
		}

		if tc.invalid {
			continue
		}

		if tc.network != network {
			t.Errorf("%v: expected %q, got %q", tc, tc.network, network)
		}

		if tc.addr != addr {
			t.Errorf("%v: expected %q, got %q", tc, tc.addr, addr)
		}
	}
}
