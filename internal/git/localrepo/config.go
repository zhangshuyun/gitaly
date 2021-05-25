package localrepo

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// Config provides functionality of the 'config' git sub-command.
type Config struct {
	repo *Repo
}

// Add adds a new entry to the repository's configuration.
func (cfg Config) Add(ctx context.Context, name, value string, opts git.ConfigAddOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	if err := cfg.repo.ExecAndWait(ctx, git.SubCmd{
		Name:  "config",
		Flags: append(buildConfigAddOptsFlags(opts), git.Flag{Name: "--add"}),
		Args:  []string{name, value},
	}); err != nil {
		// Please refer to https://git-scm.com/docs/git-config#_description
		// on return codes.
		switch {
		case isExitWithCode(err, 1):
			// section or key is invalid
			return fmt.Errorf("%w: bad section or name", git.ErrInvalidArg)
		case isExitWithCode(err, 2):
			// no section or name was provided
			return fmt.Errorf("%w: missing section or name", git.ErrInvalidArg)
		}

		return err
	}

	return nil
}

func buildConfigAddOptsFlags(opts git.ConfigAddOpts) []git.Option {
	var flags []git.Option
	if opts.Type != git.ConfigTypeDefault {
		flags = append(flags, git.Flag{Name: opts.Type.String()})
	}

	return flags
}

// GetRegexp gets all config entries which whose keys match the given regexp.
func (cfg Config) GetRegexp(ctx context.Context, nameRegexp string, opts git.ConfigGetRegexpOpts) ([]git.ConfigPair, error) {
	if err := validateNotBlank(nameRegexp, "nameRegexp"); err != nil {
		return nil, err
	}

	data, err := cfg.getRegexp(ctx, nameRegexp, opts)
	if err != nil {
		return nil, err
	}

	return cfg.parseConfig(data, opts)
}

func (cfg Config) getRegexp(ctx context.Context, nameRegexp string, opts git.ConfigGetRegexpOpts) ([]byte, error) {
	var stderr, stdout bytes.Buffer

	if err := cfg.repo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "config",
			// '--null' is used to support proper parsing of the multiline config values
			Flags: append(buildConfigGetRegexpOptsFlags(opts), git.Flag{Name: "--null"}, git.Flag{Name: "--get-regexp"}),
			Args:  []string{nameRegexp},
		},
		git.WithStderr(&stderr),
		git.WithStdout(&stdout),
	); err != nil {
		switch {
		case isExitWithCode(err, 1):
			// when no configuration values found it exits with code '1'
			return nil, nil
		case isExitWithCode(err, 6):
			// use of invalid regexp
			return nil, fmt.Errorf("%w: regexp has a bad format", git.ErrInvalidArg)
		default:
			if strings.Contains(stderr.String(), "invalid unit") ||
				strings.Contains(stderr.String(), "bad boolean config value") {
				return nil, fmt.Errorf("%w: fetched result doesn't correspond to requested type", git.ErrInvalidArg)
			}
		}

		return nil, err
	}

	return stdout.Bytes(), nil
}

func buildConfigGetRegexpOptsFlags(opts git.ConfigGetRegexpOpts) []git.Option {
	var flags []git.Option
	if opts.Type != git.ConfigTypeDefault {
		flags = append(flags, git.Flag{Name: opts.Type.String()})
	}

	if opts.ShowOrigin {
		flags = append(flags, git.Flag{Name: "--show-origin"})
	}

	if opts.ShowScope {
		flags = append(flags, git.Flag{Name: "--show-scope"})
	}

	return flags
}

func (cfg Config) parseConfig(data []byte, opts git.ConfigGetRegexpOpts) ([]git.ConfigPair, error) {
	var res []git.ConfigPair
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

		res = append(res, git.ConfigPair{
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

// Unset unsets the given config entry.
func (cfg Config) Unset(ctx context.Context, name string, opts git.ConfigUnsetOpts) error {
	if err := cfg.repo.ExecAndWait(ctx, git.SubCmd{
		Name:  "config",
		Flags: buildConfigUnsetOptsFlags(opts),
		Args:  []string{name},
	}); err != nil {
		// Please refer to https://git-scm.com/docs/git-config#_description
		// on return codes.
		switch {
		case isExitWithCode(err, 1):
			// section or key is invalid
			return fmt.Errorf("%w: bad section or name", git.ErrInvalidArg)
		case isExitWithCode(err, 2):
			// no section or name was provided
			return fmt.Errorf("%w: missing section or name", git.ErrInvalidArg)
		case isExitWithCode(err, 5):
			// unset an option which does not exist
			if opts.NotStrict {
				return nil
			}

			return git.ErrNotFound
		}
		return err
	}

	return nil
}

func buildConfigUnsetOptsFlags(opts git.ConfigUnsetOpts) []git.Option {
	if opts.All {
		return []git.Option{git.Flag{Name: "--unset-all"}}
	}

	return []git.Option{git.Flag{Name: "--unset"}}
}

func chompNul(b []byte) string {
	return string(bytes.Trim(b, "\x00"))
}

func isExitWithCode(err error, code int) bool {
	actual, ok := command.ExitStatus(err)
	if !ok {
		return false
	}

	return code == actual
}
