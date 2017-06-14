package build

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

type Config struct {
	BuildDir    string `json:"build_dir"`
	Package     string `json:"package"`
	BuildGopath string
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

	for _, s := range []string{result.BuildDir, result.Package} {
		if len(s) == 0 {
			return nil, fmt.Errorf("invalid build config: %q", data)
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	result.BuildGopath = path.Join(cwd, result.BuildDir)
	return result, nil
}

func (c *Config) PackageBuildDir() string {
	return path.Join(c.BuildGopath, "src", c.Package)
}
