package git

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
)

// LocalRepositoryConfig provides functionality of the 'config' git sub-command.
type LocalRepositoryConfig struct {
	repo repository.GitRepo
}

// Add adds a new entry to the repository's configuration.
func (repo LocalRepositoryConfig) Add(ctx context.Context, name, value string, opts ConfigAddOpts) error {
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

// GetRegexp gets all config entries which whose keys match the given regexp.
func (repo LocalRepositoryConfig) GetRegexp(ctx context.Context, nameRegexp string, opts ConfigGetRegexpOpts) ([]ConfigPair, error) {
	if err := validateNotBlank(nameRegexp, "nameRegexp"); err != nil {
		return nil, err
	}

	data, err := repo.getRegexp(ctx, nameRegexp, opts)
	if err != nil {
		return nil, err
	}

	return repo.parseConfig(data, opts)
}

func (repo LocalRepositoryConfig) getRegexp(ctx context.Context, nameRegexp string, opts ConfigGetRegexpOpts) ([]byte, error) {
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

func (repo LocalRepositoryConfig) parseConfig(data []byte, opts ConfigGetRegexpOpts) ([]ConfigPair, error) {
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

// Unset unsets the given config entry.
func (repo LocalRepositoryConfig) Unset(ctx context.Context, name string, opts ConfigUnsetOpts) error {
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

func isExitWithCode(err error, code int) bool {
	actual, ok := command.ExitStatus(err)
	if !ok {
		return false
	}

	return code == actual
}
