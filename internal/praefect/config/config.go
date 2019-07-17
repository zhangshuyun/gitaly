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
	ListenAddr string `toml:"listen_addr"`
	SocketPath string `toml:"socket_path"`

	StorageNodes []*models.StorageNode `toml:"storage_node"`

	// Whitelist is a list of relative project paths (paths comprised of project
	// hashes) that are permitted to use high availability features
	Whitelist []string `toml:"whitelist"`

	Logging              config.Logging `toml:"logging"`
	PrometheusListenAddr string         `toml:"prometheus_listen_addr"`
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
	errNoListener          = errors.New("no listen address or socket path configured")
	errNoGitalyServers     = errors.New("no primary gitaly backends configured")
	errDuplicateGitalyAddr = errors.New("gitaly listen addresses are not unique")
	errGitalyWithoutAddr   = errors.New("all gitaly nodes must have an address")
)

// Validate establishes if the config is valid
func (c Config) Validate() error {
	if c.ListenAddr == "" && c.SocketPath == "" {
		return errNoListener
	}

	if len(c.StorageNodes) == 0 {
		return errNoGitalyServers
	}

	listenAddrs := make(map[string]bool, len(c.StorageNodes))
	for _, node := range c.StorageNodes {
		if node.Address == "" {
			return errGitalyWithoutAddr
		}

		if _, found := listenAddrs[node.Address]; found {
			return errDuplicateGitalyAddr
		}

		listenAddrs[node.Address] = true
	}

	return nil
}
