package git

import (
	"context"
)

// Config represents 'config' sub-command.
// https://git-scm.com/docs/git-config
type Config interface {
	// Set will set a configuration value. Any preexisting values will be overwritten with the
	// new value.
	Set(ctx context.Context, name, value string) error

	// GetRegexp returns configurations matched to nameRegexp regular expression.
	// https://git-scm.com/docs/git-config#Documentation/git-config.txt---get-regexp
	GetRegexp(ctx context.Context, nameRegexp string, opts ConfigGetRegexpOpts) ([]ConfigPair, error)

	// Unset removes configuration associated with the name.
	// If All option is set all configurations associated with the name will be removed.
	// If multiple values associated with the name and called without All option will result in ErrNotFound error.
	// https://git-scm.com/docs/git-config#Documentation/git-config.txt---unset-all
	Unset(ctx context.Context, name string, opts ConfigUnsetOpts) error
}

// ConfigType represents supported types of the config values.
type ConfigType string

func (t ConfigType) String() string {
	return string(t)
}

var (
	// ConfigTypeDefault is a default choice.
	ConfigTypeDefault = ConfigType("")
	// ConfigTypeInt is an integer type check.
	// https://git-scm.com/docs/git-config/2.6.7#Documentation/git-config.txt---int
	ConfigTypeInt = ConfigType("--int")
	// ConfigTypeBool is a bool type check.
	// https://git-scm.com/docs/git-config/2.6.7#Documentation/git-config.txt---bool
	ConfigTypeBool = ConfigType("--bool")
	// ConfigTypeBoolOrInt is a bool or int type check.
	// https://git-scm.com/docs/git-config/2.6.7#Documentation/git-config.txt---bool-or-int
	ConfigTypeBoolOrInt = ConfigType("--bool-or-int")
	// ConfigTypePath is a path type check.
	// https://git-scm.com/docs/git-config/2.6.7#Documentation/git-config.txt---path
	ConfigTypePath = ConfigType("--path")
)

// ConfigGetRegexpOpts is used to configure invocation of the 'git config --get-regexp' command.
type ConfigGetRegexpOpts struct {
	// Type allows to specify an expected type for the configuration.
	Type ConfigType
	// ShowOrigin controls if origin needs to be fetched.
	ShowOrigin bool
	// ShowScope controls if scope needs to be fetched.
	ShowScope bool
}

// ConfigUnsetOpts allows to configure fetching of the configurations using regexp.
type ConfigUnsetOpts struct {
	// All controls if all values associated with the key needs to be unset.
	All bool
	// NotStrict if set to true it won't return an error if the configuration was not found
	// or in case multiple values exist for a given key and All option is not set.
	NotStrict bool
}
