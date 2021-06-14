package blackbox

import (
	"fmt"
	"net/url"
	"time"

	"github.com/pelletier/go-toml"
	logconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
)

// Config is the configuration for gitaly-blackbox.
type Config struct {
	// PrometheusListenAddr is the listen address on which Prometheus metrics should be
	// made available for clients.
	PrometheusListenAddr string `toml:"prometheus_listen_addr"`
	// Sleep is the number of seconds between probe runs.
	Sleep int `toml:"sleep"`
	// sleepDuration is the same as Sleep but converted to a proper duration.
	sleepDuration time.Duration
	// Logging configures logging.
	Logging logconfig.Config `toml:"logging"`
	// Probes defines endpoints to probe. At least one probe must be defined.
	Probes []Probe `toml:"probe"`
}

// Probe is the configuration for a specific endpoint whose clone performance should be exercised.
type Probe struct {
	// Name is the name of the probe. This is used both for logging and for exported
	// Prometheus metrics.
	Name string `toml:"name"`
	// URL is the URL of the Git repository that should be probed. For now, only the
	// HTTP transport is supported.
	URL string `toml:"url"`
	// User is the user to authenticate as when connecting to the repository.
	User string `toml:"user"`
	// Password is the password to authenticate with when connecting to the repository.
	// Note that this password may easily leak when connecting to a non-HTTPS URL.
	Password string `toml:"password"`
}

// ParseConfig parses the provided TOML-formatted configuration string and either returns the
// parsed configuration or an error.
func ParseConfig(raw string) (Config, error) {
	config := Config{}
	if err := toml.Unmarshal([]byte(raw), &config); err != nil {
		return Config{}, err
	}

	if config.PrometheusListenAddr == "" {
		return Config{}, fmt.Errorf("missing prometheus_listen_addr")
	}

	if config.Sleep < 0 {
		return Config{}, fmt.Errorf("sleep time is less than 0")
	}
	if config.Sleep == 0 {
		config.Sleep = 15 * 60
	}
	config.sleepDuration = time.Duration(config.Sleep) * time.Second

	if len(config.Probes) == 0 {
		return Config{}, fmt.Errorf("must define at least one probe")
	}

	for _, probe := range config.Probes {
		if len(probe.Name) == 0 {
			return Config{}, fmt.Errorf("all probes must have a 'name' attribute")
		}

		parsedURL, err := url.Parse(probe.URL)
		if err != nil {
			return Config{}, err
		}

		if s := parsedURL.Scheme; s != "http" && s != "https" {
			return Config{}, fmt.Errorf("unsupported probe URL scheme: %v", probe.URL)
		}
	}

	return config, nil
}
