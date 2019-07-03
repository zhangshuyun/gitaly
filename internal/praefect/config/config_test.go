package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	primarySrv := &GitalyServer{"test", "localhost:23456"}
	secondarySrvs := []*GitalyServer{
		{"test1", "localhost:23457"},
		{"test2", "localhost:23458"},
	}

	testCases := []struct {
		desc   string
		config Config
		err    error
	}{
		{
			desc:   "No ListenAddr or SocketPath",
			config: Config{ListenAddr: "", Servers: append([]*GitalyServer{primarySrv}, secondarySrvs...)},
			err:    errNoListener,
		},
		{
			desc:   "Only a SocketPath",
			config: Config{SocketPath: "/tmp/praefect.socket", Servers: append([]*GitalyServer{primarySrv}, secondarySrvs...)},
			err:    nil,
		},
		{
			desc:   "No servers",
			config: Config{ListenAddr: "localhost:1234"},
			err:    errNoGitalyServers,
		},
		{
			desc:   "duplicate address",
			config: Config{ListenAddr: "localhost:1234", Servers: []*GitalyServer{primarySrv, primarySrv}},
			err:    errDuplicateGitalyAddr,
		},
		{
			desc:   "Valid config",
			config: Config{ListenAddr: "localhost:1234", Servers: append([]*GitalyServer{primarySrv}, secondarySrvs...)},
			err:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.config.Validate()
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestConfigParsing(t *testing.T) {
	testCases := []struct {
		filePath string
		expected Config
	}{
		{
			filePath: "testdata/config.toml",
			expected: Config{
				Servers: []*GitalyServer{
					{
						Name:       "default",
						ListenAddr: "tcp://gitaly-primary.example.com",
					},
					{
						Name:       "default",
						ListenAddr: "tcp://gitaly-backup1.example.com",
					},
					{
						Name:       "backup",
						ListenAddr: "tcp://gitaly-backup2.example.com",
					},
				},
				Whitelist: []string{
					"abcd1234",
					"edfg5678",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.filePath, func(t *testing.T) {
			cfg, err := FromFile(tc.filePath)
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg)
		})
	}
}
