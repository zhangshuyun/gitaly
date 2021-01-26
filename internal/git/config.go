package git

import (
	"context"
)

// Config represents 'config' sub-command.
// https://git-scm.com/docs/git-config
type Config interface {
	// Add adds a new configuration value.
	// WARNING: you can't ever use it for anything that contains secrets.
	// https://git-scm.com/docs/git-config#Documentation/git-config.txt---add
	Add(ctx context.Context, name, value string, opts ConfigAddOpts) error

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

// ConfigAddOpts is used to configure invocation of the 'git config --add' command.
type ConfigAddOpts struct {
	// Type controls rules used to check the value.
	Type ConfigType
}

func (opts ConfigAddOpts) buildFlags() []Option {
	var flags []Option
	if opts.Type != ConfigTypeDefault {
		flags = append(flags, Flag{Name: opts.Type.String()})
	}

	return flags
}

// ConfigGetRegexpOpts is used to configure invocation of the 'git config --get-regexp' command.
type ConfigGetRegexpOpts struct {
	// Type allows to specify an expected type for the configuration.
	Type ConfigType
	// ShowOrigin controls if origin needs to be fetched.
	ShowOrigin bool
	// ShowScope controls if scope needs to be fetched.
	ShowScope bool
}

func (opts ConfigGetRegexpOpts) buildFlags() []Option {
	var flags []Option
	if opts.Type != ConfigTypeDefault {
		flags = append(flags, Flag{Name: opts.Type.String()})
	}

	if opts.ShowOrigin {
		flags = append(flags, Flag{Name: "--show-origin"})
	}

	if opts.ShowScope {
		flags = append(flags, Flag{Name: "--show-scope"})
	}

	return flags
}

// ConfigUnsetOpts allows to configure fetching of the configurations using regexp.
type ConfigUnsetOpts struct {
	// All controls if all values associated with the key needs to be unset.
	All bool
	// NotStrict if set to true it won't return an error if the configuration was not found
	// or in case multiple values exist for a given key and All option is not set.
	NotStrict bool
}

func (opts ConfigUnsetOpts) buildFlags() []Option {
	if opts.All {
		return []Option{Flag{Name: "--unset-all"}}
	}

	return []Option{Flag{Name: "--unset"}}
}
