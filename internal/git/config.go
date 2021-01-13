package git

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
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

// RepositoryConfig provides functionality of the 'config' git sub-command.
type RepositoryConfig struct {
	repo repository.GitRepo
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

func (repo RepositoryConfig) Add(ctx context.Context, name, value string, opts ConfigAddOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	cmd, err := NewCommand(ctx, repo.repo, nil, SubCmd{
		Name:  "config",
		Flags: append(opts.buildFlags(), Flag{Name: "--add"}),
		Args:  []string{name, value},
	})
	if err != nil {
		return err
	}

	// Please refer to https://git-scm.com/docs/git-config#_description on return codes.
	if err := cmd.Wait(); err != nil {
		switch {
		case isExitWithCode(err, 1):
			// section or key is invalid
			return fmt.Errorf("%w: bad section or name", ErrInvalidArg)
		case isExitWithCode(err, 2):
			// no section or name was provided
			return fmt.Errorf("%w: missing section or name", ErrInvalidArg)
		}

		return err
	}

	return nil
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

func (repo RepositoryConfig) GetRegexp(ctx context.Context, nameRegexp string, opts ConfigGetRegexpOpts) ([]ConfigPair, error) {
	if err := validateNotBlank(nameRegexp, "nameRegexp"); err != nil {
		return nil, err
	}

	data, err := repo.getRegexp(ctx, nameRegexp, opts)
	if err != nil {
		return nil, err
	}

	return repo.parseConfig(data, opts)
}

func (repo RepositoryConfig) getRegexp(ctx context.Context, nameRegexp string, opts ConfigGetRegexpOpts) ([]byte, error) {
	var stderr bytes.Buffer
	cmd, err := NewCommand(ctx, repo.repo, nil,
		SubCmd{
			Name: "config",
			// '--null' is used to support proper parsing of the multiline config values
			Flags: append(opts.buildFlags(), Flag{Name: "--null"}, Flag{Name: "--get-regexp"}),
			Args:  []string{nameRegexp},
		},
		WithStderr(&stderr),
	)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, fmt.Errorf("reading output: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		switch {
		case isExitWithCode(err, 1):
			// when no configuration values found it exits with code '1'
			return nil, nil
		case isExitWithCode(err, 6):
			// use of invalid regexp
			return nil, fmt.Errorf("%w: regexp has a bad format", ErrInvalidArg)
		default:
			if strings.Contains(stderr.String(), "invalid unit") {
				return nil, fmt.Errorf("%w: fetched result doesn't correspond to requested type", ErrInvalidArg)
			}
		}

		return nil, err
	}

	return data, nil
}

func (repo RepositoryConfig) parseConfig(data []byte, opts ConfigGetRegexpOpts) ([]ConfigPair, error) {
	var res []ConfigPair
	var err error

	for reader := bufio.NewReader(bytes.NewReader(data)); ; {
		// The format is: <scope> NUL <origin> NUL <KEY> NL <VALUE> NUL
		// Where the <scope> and <origin> are optional and depend on corresponding configuration options.
		var scope []byte
		if opts.ShowScope {
			if scope, err = reader.ReadBytes(0); err != nil {
				break
			}
		}

		var origin []byte
		if opts.ShowOrigin {
			if origin, err = reader.ReadBytes(0); err != nil {
				break
			}
		}

		var pair []byte
		if pair, err = reader.ReadBytes(0); err != nil {
			break
		}

		parts := bytes.SplitN(pair, []byte{'\n'}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad format of the config: %q", pair)
		}

		res = append(res, ConfigPair{
			Key:    string(parts[0]),
			Value:  chompNul(parts[1]),
			Origin: chompNul(origin),
			Scope:  chompNul(scope),
		})
	}

	if err == io.EOF {
		return res, nil
	}

	return nil, fmt.Errorf("parsing output: %w", err)
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

func (repo RepositoryConfig) Unset(ctx context.Context, name string, opts ConfigUnsetOpts) error {
	cmd, err := NewCommand(ctx, repo.repo, nil, SubCmd{
		Name:  "config",
		Flags: opts.buildFlags(),
		Args:  []string{name},
	})
	if err != nil {
		return err
	}

	// Please refer to https://git-scm.com/docs/git-config#_description on return codes.
	if err := cmd.Wait(); err != nil {
		switch {
		case isExitWithCode(err, 1):
			// section or key is invalid
			return fmt.Errorf("%w: bad section or name", ErrInvalidArg)
		case isExitWithCode(err, 2):
			// no section or name was provided
			return fmt.Errorf("%w: missing section or name", ErrInvalidArg)
		case isExitWithCode(err, 5):
			// unset an option which does not exist
			if opts.NotStrict {
				return nil
			}

			return ErrNotFound
		}
		return err
	}

	return nil
}

func chompNul(b []byte) string {
	return string(bytes.Trim(b, "\x00"))
}
