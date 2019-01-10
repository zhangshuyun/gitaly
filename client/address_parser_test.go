package client

import (
	"testing"
)

func TestParseAddress(t *testing.T) {
	testCases := []struct {
		raw       string
		canonical string
		invalid   bool
	}{
		{raw: "unix:/foo/bar.socket", canonical: "unix:/foo/bar.socket"},
		{raw: "unix://foo/bar.socket", canonical: "unix:/foo/bar.socket"},
		{raw: "unix:///foo/bar.socket", canonical: "unix:/foo/bar.socket"},
		// We explicitly want to support relative paths
		{raw: "unix:foo/bar.socket", canonical: "unix:foo/bar.socket"},
		{raw: "tcp://1.2.3.4", canonical: "1.2.3.4"},
		{raw: "tcp://1.2.3.4:567", canonical: "1.2.3.4:567"},
		{raw: "tcp://foobar", canonical: "foobar"},
		{raw: "tcp://foobar:567", canonical: "foobar:567"},
		{raw: "tcp://1.2.3.4/foo/bar.socket", invalid: true},
		{raw: "tls://1.2.3.4/foo/bar.socket", invalid: true},
		{raw: "tcp:///foo/bar.socket", invalid: true},
		{raw: "tcp:/foo/bar.socket", invalid: true},
		{raw: "tcp://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999", canonical: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999"},
		{raw: "foobar:9999", canonical: "foobar:9999"},
		// As per https://github.com/grpc/grpc/blob/master/doc/naming.md...
		{raw: "dns:///127.0.0.1:9999", canonical: "dns:///127.0.0.1:9999"},
		{raw: "dns:///[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999", canonical: "dns:///[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9999"},
	}

	for _, tc := range testCases {
		canonical, err := parseAddress(tc.raw)

		if err == nil && tc.invalid {
			t.Errorf("%v: expected error, got none", tc)
		} else if err != nil && !tc.invalid {
			t.Errorf("%v: parse error: %v", tc, err)
			continue
		}

		if tc.invalid {
			continue
		}

		if tc.canonical != canonical {
			t.Errorf("%v: expected %q, got %q", tc, tc.canonical, canonical)
		}
	}
}
