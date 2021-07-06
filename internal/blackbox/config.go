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

// ProbeType is the type of the probe.
type ProbeType string

const (
	// Fetch exercises the equivalent of git-fetch(1) with measurements for packfile negotiation
	// and receiving the packfile.
	Fetch = ProbeType("fetch")
	// Push exercises the equivalent of git-push(1) with measurements for packfile negotiation
	// and sending the packfile.
	Push = ProbeType("push")
)

// PushCommand describes a command performed as part of the push.
type PushCommand struct {
	// OldOID is the old state of the reference that should be updated.
	OldOID string `toml:"old_oid"`
	// OldOID is the new target state of the reference that should be updated.
	NewOID string `toml:"new_oid"`
	// Reference is the name of the reference that should be updated.
	Reference string `toml:"reference"`
}

// PushConfig is the configuration for a Push-type probe.
type PushConfig struct {
	// Commands is the list of commands which should be executed as part of the push.
	Commands []PushCommand `toml:"commands"`
	// Packfile is the path to the packfile that shall be sent as part of the push.
	Packfile string `toml:"packfile"`
}

// Probe is the configuration for a specific endpoint whose clone performance should be exercised.
type Probe struct {
	// Name is the name of the probe. This is used both for logging and for exported
	// Prometheus metrics.
	Name string `toml:"name"`
	// Type is the type of the probe. See ProbeType for the supported types. Defaults to `Fetch`
	// if no type was given.
	Type ProbeType `toml:"type"`
	// URL is the URL of the Git repository that should be probed. For now, only the
	// HTTP transport is supported.
	URL string `toml:"url"`
	// User is the user to authenticate as when connecting to the repository.
	User string `toml:"user"`
	// Password is the password to authenticate with when connecting to the repository.
	// Note that this password may easily leak when connecting to a non-HTTPS URL.
	Password string `toml:"password"`
	// Push contains the configuration of a Push-type probe.
	Push *PushConfig `toml:"push"`
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

		switch probe.Type {
		case "", Fetch:
			probe.Type = Fetch
			if probe.Push != nil {
				return Config{}, fmt.Errorf("fetch probe %q cannot have push configuration", probe.Name)
			}
		case Push:
			if probe.Push == nil {
				return Config{}, fmt.Errorf("push probe %q must have push configuration", probe.Name)
			}

			if len(probe.Push.Commands) == 0 {
				return Config{}, fmt.Errorf("push probe %q must have at least one command", probe.Name)
			}
		default:
			return Config{}, fmt.Errorf("unsupported probe type: %q", probe.Type)
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
