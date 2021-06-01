package blackbox

import (
	"fmt"
	"net/url"
	"time"

	"github.com/pelletier/go-toml"
	logconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
)

type Config struct {
	PrometheusListenAddr string `toml:"prometheus_listen_addr"`
	Sleep                int    `toml:"sleep"`
	SleepDuration        time.Duration
	Logging              logconfig.Config `toml:"logging"`
	Probes               []Probe          `toml:"probe"`
}

type Probe struct {
	Name     string `toml:"name"`
	URL      string `toml:"url"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

func ParseConfig(raw string) (*Config, error) {
	config := &Config{}
	if err := toml.Unmarshal([]byte(raw), config); err != nil {
		return nil, err
	}

	if config.PrometheusListenAddr == "" {
		return nil, fmt.Errorf("missing prometheus_listen_addr")
	}

	if config.Sleep < 0 {
		return nil, fmt.Errorf("sleep time is less than 0")
	}
	if config.Sleep == 0 {
		config.Sleep = 15 * 60
	}
	config.SleepDuration = time.Duration(config.Sleep) * time.Second

	if len(config.Probes) == 0 {
		return nil, fmt.Errorf("must define at least one probe")
	}

	for _, probe := range config.Probes {
		if len(probe.Name) == 0 {
			return nil, fmt.Errorf("all probes must have a 'name' attribute")
		}

		parsedURL, err := url.Parse(probe.URL)
		if err != nil {
			return nil, err
		}

		if s := parsedURL.Scheme; s != "http" && s != "https" {
			return nil, fmt.Errorf("unsupported probe URL scheme: %v", probe.URL)
		}
	}

	return config, nil
}
