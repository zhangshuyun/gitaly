package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

// Config is a container for everything found in the TOML config file
type Config struct {
	ListenAddr    string `toml:"listen_addr"`
	TLSListenAddr string `toml:"tls_listen_addr"`
	SocketPath    string `toml:"socket_path"`

	Nodes []*models.Node `toml:"node"`

	Logging              config.Logging `toml:"logging"`
	PrometheusListenAddr string         `toml:"prometheus_listen_addr"`
	Auth                 Auth           `toml:"auth"`
	TLS                  TLS            `toml:"tls"`
}

// TLS contains details about the tls configuration
type TLS struct {
	CertificatePath string `toml:"certificate_path"`
	KeyPath         string `toml:"key_path"`
}

// Auth contains details about the authentication configuration
type Auth struct {
	Token string `toml:"token"`
}

// FromFile loads the config for the passed file path
func FromFile(filePath string) (Config, error) {
	config := &Config{}
	cfgFile, err := os.Open(filePath)
	if err != nil {
		return *config, err
	}
	defer cfgFile.Close()

	_, err = toml.DecodeReader(cfgFile, config)
	return *config, err
}

var (
	errNoListener           = errors.New("no listen address or socket path configured")
	errNoGitalyServers      = errors.New("no primary gitaly backends configured")
	errDuplicateStorage     = errors.New("internal gitaly storages are not unique")
	errGitalyWithoutAddr    = errors.New("all gitaly nodes must have an address")
	errGitalyWithoutStorage = errors.New("all gitaly nodes must have a storage")
	errMoreThanOnePrimary   = errors.New("only 1 node can be designated as a primary")
	errNoPrimaries          = errors.New("no primaries designated")
)

// Validate establishes if the config is valid
func (c Config) Validate() error {
	if c.ListenAddr == "" && c.SocketPath == "" {
		return errNoListener
	}

	storages := make(map[string]struct{})

	var primaries int
	for _, node := range c.Nodes {
		if node.DefaultPrimary {
			primaries++
		}

		if primaries > 1 {
			return errMoreThanOnePrimary
		}
		if node.Storage == "" {
			return errGitalyWithoutStorage
		}

		if node.Address == "" {
			return errGitalyWithoutAddr
		}

		if _, found := storages[node.Storage]; found {
			return errDuplicateStorage
		}

		storages[node.Storage] = struct{}{}
	}

	if len(storages) == 0 {
		return errNoGitalyServers
	}

	if primaries == 0 {
		return errNoPrimaries
	}

	return nil
}
