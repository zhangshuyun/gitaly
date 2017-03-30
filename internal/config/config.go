package config

import (
	"fmt"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	Config     *config
	configFile string
)

func init() {
	flag.String("config", "", "Configfile to load")
	flag.Parse()

	viper.SetEnvPrefix("gitaly") // Will be upper-cased by default
	viper.BindEnv("config")
	viper.BindPFlag("config", flag.Lookup("config"))

	configFile := viper.GetString("config")

	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal: read config file: %q", configFile))
	}
}

type Storage struct {
	Name string `toml:"name"`
	Path string `toml:"path"`
}

type config struct {
	Storages []Storage `toml:"storage"`
}
