package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"

	"gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

// Config is a container for everything found in the TOML config file
type Config struct {
	ListenAddr      string            `toml:"listen_addr"`
	SocketPath      string            `toml:"socket_path"`
	VirtualStorages []*VirtualStorage `toml:"virtual_storage"`
	//TODO: Remove VirtualStorageName and Nodes once omnibus and gdk are updated with support for
	// VirtualStorages
	VirtualStorageName   string         `toml:"virtual_storage_name"`
	Nodes                []*models.Node `toml:"node"`
	Logging              log.Config     `toml:"logging"`
	Sentry               sentry.Config  `toml:"sentry"`
	PrometheusListenAddr string         `toml:"prometheus_listen_addr"`
	Auth                 auth.Config    `toml:"auth"`
}

// VirtualStorage represents a set of nodes for a storage
type VirtualStorage struct {
	Name  string         `toml:"name"`
	Nodes []*models.Node `toml:"node"`
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

	// TODO: Remove this after the virtual storages change is merged in omnibus
	// and gdk. This is for backwards compatibility purposes only
	if len(config.VirtualStorages) == 0 && config.VirtualStorageName != "" && len(config.Nodes) > 0 {
		config.VirtualStorages = []*VirtualStorage{
			&VirtualStorage{
				Name:  config.VirtualStorageName,
				Nodes: config.Nodes,
			},
		}
		config.VirtualStorageName = ""
		config.Nodes = nil
	}

	return *config, err
}

var (
	errDuplicateStorage         = errors.New("internal gitaly storages are not unique")
	errGitalyWithoutAddr        = errors.New("all gitaly nodes must have an address")
	errGitalyWithoutStorage     = errors.New("all gitaly nodes must have a storage")
	errMoreThanOnePrimary       = errors.New("only 1 node can be designated as a primary")
	errNoGitalyServers          = errors.New("no primary gitaly backends configured")
	errNoListener               = errors.New("no listen address or socket path configured")
	errNoPrimaries              = errors.New("no primaries designated")
	errNoVirtualStorages        = errors.New("no virtual storages configured")
	errStorageAddressMismatch   = errors.New("storages with the same name must have the same address")
	errVirtualStoragesNotUnique = errors.New("virtual storages must have unique names")
)

// Validate establishes if the config is valid
func (c Config) Validate() error {
	if c.ListenAddr == "" && c.SocketPath == "" {
		return errNoListener
	}

	if len(c.VirtualStorages) == 0 {
		return errNoVirtualStorages
	}

	allStorages := make(map[string]string)
	virtualStorages := make(map[string]struct{})

	for _, virtualStorage := range c.VirtualStorages {
		if _, ok := virtualStorages[virtualStorage.Name]; ok {
			return errVirtualStoragesNotUnique
		}

		virtualStorages[virtualStorage.Name] = struct{}{}

		storages := make(map[string]struct{})
		var primaries int
		for _, node := range virtualStorage.Nodes {
			if node.DefaultPrimary {
				primaries++
			}

			if primaries > 1 {
				return fmt.Errorf("virtual storage %s: %v", virtualStorage.Name, errMoreThanOnePrimary)
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

			if address, found := allStorages[node.Storage]; found {
				if address != node.Address {
					return errStorageAddressMismatch
				}
			} else {
				allStorages[node.Storage] = node.Address
			}

			storages[node.Storage] = struct{}{}
		}

		if primaries == 0 {
			return fmt.Errorf("virtual storage %s: %v", virtualStorage.Name, errNoPrimaries)
		}
		if len(storages) == 0 {
			return fmt.Errorf("virtual storage %s: %v", virtualStorage.Name, errNoGitalyServers)
		}
	}

	return nil
}
