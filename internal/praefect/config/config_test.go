package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

func TestConfigValidation(t *testing.T) {
	nodes := []*models.Node{
		{ID: 1, Storage: "internal-1", Address: "localhost:23456", Token: "secret-token", DefaultPrimary: true},
		{ID: 2, Storage: "internal-2", Address: "localhost:23457", Token: "secret-token"},
		{ID: 3, Storage: "internal-3", Address: "localhost:23458", Token: "secret-token"},
	}

	testCases := []struct {
		desc   string
		config Config
		err    error
	}{
		{
			desc:   "No ListenAddr or SocketPath",
			config: Config{ListenAddr: "", Nodes: nodes},
			err:    errNoListener,
		},
		{
			desc:   "Only a SocketPath",
			config: Config{SocketPath: "/tmp/praefect.socket", Nodes: nodes},
			err:    nil,
		},
		{
			desc:   "No servers",
			config: Config{ListenAddr: "localhost:1234"},
			err:    errNoGitalyServers,
		},
		{
			desc:   "duplicate storage",
			config: Config{ListenAddr: "localhost:1234", Nodes: append(nodes, &models.Node{Storage: nodes[0].Storage, Address: nodes[1].Address})},
			err:    errDuplicateStorage,
		},
		{
			desc:   "Valid config",
			config: Config{ListenAddr: "localhost:1234", Nodes: nodes},
			err:    nil,
		},
		{
			desc:   "No designated primaries",
			config: Config{ListenAddr: "localhost:1234", Nodes: nodes[1:]},
			err:    errNoPrimaries,
		},
		{
			desc:   "More than 1 primary",
			config: Config{ListenAddr: "localhost:1234", Nodes: append(nodes, &models.Node{ID: 3, Storage: "internal-4", Address: "localhost:23459", Token: "secret-token", DefaultPrimary: true})},
			err:    errMoreThanOnePrimary,
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
				VirtualStorageName: "praefect",
				Logging: log.Config{
					Level:  "info",
					Format: "json",
				},
				Sentry: sentry.Config{
					DSN:         "abcd123",
					Environment: "production",
				},
				Nodes: []*models.Node{
					&models.Node{
						Address:        "tcp://gitaly-internal-1.example.com",
						Storage:        "praefect-internal-1",
						DefaultPrimary: true,
					},
					{
						Address: "tcp://gitaly-internal-2.example.com",
						Storage: "praefect-internal-2",
					},
					{
						Address: "tcp://gitaly-internal-3.example.com",
						Storage: "praefect-internal-3",
					},
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
