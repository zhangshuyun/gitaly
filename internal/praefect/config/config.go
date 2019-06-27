package config

import (
	"errors"
	"os"

	"github.com/BurntSushi/toml"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Config is a container for everything found in the TOML config file
type Config struct {
	ListenAddr string `toml:"listen_addr"`
	SocketPath string `toml:"socket_path"`

	PrimaryServer    *GitalyServer   `toml:"primary_server"`
	SecondaryServers []*GitalyServer `toml:"secondary_server"`

	// Whitelist is a list of relative project paths (paths comprised of project
	// hashes) that are permitted to use high availability features
	Whitelist []string `toml:"whitelist"`

	Logging              config.Logging `toml:"logging"`
	PrometheusListenAddr string         `toml:"prometheus_listen_addr"`
	Admin                *Admin         `toml:"praefect_admin"`
}

// GitalyServer allows configuring the servers that RPCs are proxied to
type GitalyServer struct {
	Name       string `toml:"name"`
	ListenAddr string `toml:"listen_addr" split_words:"true"`
}

// Admin configures the admin service
type Admin struct {
	Port int `toml:"port"`
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
	errGitalyWithoutName   = errors.New("all gitaly servers must have a name")
)

var emptyServer = &GitalyServer{}

// Validate establishes if the config is valid
func (c Config) Validate() error {
	if c.ListenAddr == "" && c.SocketPath == "" {
		return errNoListener
	}

	if c.PrimaryServer == nil || c.PrimaryServer == emptyServer {
		return errNoGitalyServers
	}

	listenAddrs := make(map[string]bool, len(c.SecondaryServers)+1)
	for _, gitaly := range append(c.SecondaryServers, c.PrimaryServer) {
		if gitaly.Name == "" {
			return errGitalyWithoutName
		}

		if _, found := listenAddrs[gitaly.ListenAddr]; found {
			return errDuplicateGitalyAddr
		}

		listenAddrs[gitaly.ListenAddr] = true
	}

	return nil
}
