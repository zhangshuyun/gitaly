package build

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
)

type Config struct {
	BuildGopath string `json:"build_gopath"`
	Package     string `json:"package"`
}

func ReadConfig() (*Config, error) {
	data, err := ioutil.ReadFile("build_config.json")
	if err != nil {
		return nil, err
	}
	result := &Config{}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}

	for _, s := range []string{result.BuildGopath, result.Package} {
		if len(s) == 0 {
			return nil, fmt.Errorf("invalid build config: %q", data)
		}
	}

	return result, nil
}

func (c *Config) PackageBuildDir() string {
	return path.Join(c.BuildGopath, "src", c.Package)
}
