package config

import (
	"fmt"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var (
	Config     config
	configFile string
)

func init() {
	flag.String("config", "", "Configfile to load")
	flag.Parse()

	viper.SetEnvPrefix("gitaly") // Will be upper-cased by default
	viper.BindEnv("config")
	viper.BindPFlag("config", flag.Lookup("config"))

	configFile := viper.GetString("config")

	if len(configFile) == 0 {
		panic(fmt.Errorf("fatal: no config file given"))
	}

	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal: read config file: %q", configFile))
	}

	err = viper.Unmarshal(&Config)
	if err != nil {
		panic(fmt.Errorf("fatal: unable to decode config: %q: %v", configFile, err))
	}
}

type Storage struct {
	Name string
	Path string
}

type config struct {
	Storages []Storage `mapstructure:"storage"`
}
