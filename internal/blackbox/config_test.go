package blackbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigParseFailures(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
	}{
		{desc: "empty config"},
		{desc: "probe without name", in: "[[probe]]\n"},
		{desc: "unsupported probe url", in: "[[probe]]\nname='foo'\nurl='ssh://not:supported'"},
		{desc: "missing probe url", in: "[[probe]]\nname='foo'\n"},
		{desc: "negative sleep", in: "sleep=-1\n[[probe]]\nname='foo'\nurl='http://foo/bar'"},
		{desc: "no listen addr", in: "[[probe]]\nname='foo'\nurl='http://foo/bar'"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := ParseConfig(tc.in)

			require.Error(t, err, "expect parse error")
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
