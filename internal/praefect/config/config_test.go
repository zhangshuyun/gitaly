package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/models"
)

func TestConfigValidation(t *testing.T) {
	primarySrv := &models.GitalyServer{"test", "localhost:23456", "secret-token"}
	secondarySrvs := []*models.GitalyServer{
		{"test1", "localhost:23457", "secret-token"},
		{"test2", "localhost:23458", "secret-token"},
	}

	testCases := []struct {
		desc   string
		config Config
		err    error
	}{
		{
			desc:   "No ListenAddr or SocketPath",
			config: Config{ListenAddr: "", PrimaryServer: primarySrv, SecondaryServers: secondarySrvs},
			err:    errNoListener,
		},
		{
			desc:   "Only a SocketPath",
			config: Config{SocketPath: "/tmp/praefect.socket", PrimaryServer: primarySrv, SecondaryServers: secondarySrvs},
			err:    nil,
		},
		{
			desc:   "No servers",
			config: Config{ListenAddr: "localhost:1234"},
			err:    errNoGitalyServers,
		},
		{
			desc:   "duplicate address",
			config: Config{ListenAddr: "localhost:1234", PrimaryServer: primarySrv, SecondaryServers: []*models.GitalyServer{primarySrv}},
			err:    errDuplicateGitalyAddr,
		},
		{
			desc:   "Valid config",
			config: Config{ListenAddr: "localhost:1234", PrimaryServer: primarySrv, SecondaryServers: secondarySrvs},
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
				PrimaryServer: &models.GitalyServer{
					Name:       "default",
					ListenAddr: "tcp://gitaly-primary.example.com",
				},
				SecondaryServers: []*models.GitalyServer{
					{
						Name:       "default",
						ListenAddr: "tcp://gitaly-backup1.example.com",
					},
					{
						Name:       "backup",
						ListenAddr: "tcp://gitaly-backup2.example.com",
					},
				},
				Whitelist: []string{"abcd1234", "edfg5678"},
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
