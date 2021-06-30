package blackbox

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigParseFailures(t *testing.T) {
	testCases := []struct {
		desc        string
		in          string
		expectedErr error
	}{
		{
			desc:        "empty config",
			expectedErr: errors.New("missing prometheus_listen_addr"),
		},
		{
			desc:        "probe without name",
			in:          "prometheus_listen_addr = 'foo'\n[[probe]]\n",
			expectedErr: errors.New("all probes must have a 'name' attribute"),
		},
		{
			desc: "unsupported probe url",
			in:   "prometheus_listen_addr = 'foo'\n[[probe]]\nname='foo'\nurl='ssh://not:supported'",
			expectedErr: &url.Error{
				Op:  "parse",
				URL: "ssh://not:supported",
				Err: errors.New("invalid port \":supported\" after host"),
			},
		},
		{
			desc:        "missing probe url",
			in:          "prometheus_listen_addr = 'foo'\n[[probe]]\nname='foo'\n",
			expectedErr: errors.New("unsupported probe URL scheme: "),
		},
		{
			desc:        "negative sleep",
			in:          "prometheus_listen_addr = 'foo'\nsleep=-1\n[[probe]]\nname='foo'\nurl='http://foo/bar'",
			expectedErr: errors.New("sleep time is less than 0"),
		},
		{
			desc:        "no listen addr",
			in:          "[[probe]]\nname='foo'\nurl='http://foo/bar'",
			expectedErr: errors.New("missing prometheus_listen_addr"),
		},
		{
			desc:        "invalid probe type",
			in:          "prometheus_listen_addr = 'foo'\n[[probe]]\nname='foo'\nurl='http://foo/bar'\ntype='foo'",
			expectedErr: errors.New("unsupported probe type: \"foo\""),
		},
		{
			desc: "valid configuration",
			in:   "prometheus_listen_addr = 'foo'\n[[probe]]\nname='foo'\nurl='http://foo/bar'\n",
		},
		{
			desc: "valid configuration with explicit type",
			in:   "prometheus_listen_addr = 'foo'\n[[probe]]\nname='foo'\nurl='http://foo/bar'\ntype='fetch'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ParseConfig(tc.in)
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestConfigSleep(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
		out  time.Duration
	}{
		{
			desc: "default sleep time",
			out:  15 * time.Minute,
		},
		{
			desc: "1 second",
			in:   "sleep = 1\n",
			out:  time.Second,
		},
	}

	const validConfig = `
prometheus_listen_addr = ':9687'
[[probe]]
name = 'foo'
url = 'http://foo/bar'
`
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := ParseConfig(tc.in + validConfig)
			require.NoError(t, err, "parse config")

			require.Equal(t, tc.out, cfg.sleepDuration, "parsed sleep time")
		})
	}
}
