package localrepo

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
)

// Config provides functionality of the 'config' git sub-command.
type Config struct {
	repo *Repo
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

// SetConfig will set a configuration value. Any preexisting values will be overwritten with the new
// value. The change will use transactional semantics.
func (repo *Repo) SetConfig(ctx context.Context, key, value string, txManager transaction.Manager) (returnedErr error) {
	repoPath, err := repo.Path()
	if err != nil {
		return err
	}
	configPath := filepath.Join(repoPath, "config")

	writer, err := safe.NewLockingFileWriter(configPath, safe.LockingFileWriterConfig{
		SeedContents: true,
	})
	if err != nil {
		return fmt.Errorf("creating config writer: %w", err)
	}
	defer func() {
		if err := writer.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("closing config writer: %w", err)
		}
	}()

	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "config",
		Flags: []git.Option{
			git.Flag{Name: "--replace-all"},
			git.ValueFlag{Name: "--file", Value: writer.Path()},
		},
		Args: []string{key, value},
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
	}

	if err := transaction.CommitLockedFile(ctx, txManager, writer); err != nil {
		return fmt.Errorf("committing config: %w", err)
	}

	return nil
}

// UnsetMatchingConfig removes all config entries whose key match the given regular expression. If
// no matching keys are found, then this function returns an `git.ErrNotFound` error. The change
// will use transactional semantics.
func (repo *Repo) UnsetMatchingConfig(
	ctx context.Context,
	regex string,
	txManager transaction.Manager,
) (returnedErr error) {
	// An empty regular expression would match every key and is thus quite dangerous.
	if err := validateNotBlank(regex, "regex"); err != nil {
		return err
	}

	repoPath, err := repo.Path()
	if err != nil {
		return fmt.Errorf("getting repo path: %w", err)
	}

	writer, err := safe.NewLockingFileWriter(filepath.Join(repoPath, "config"), safe.LockingFileWriterConfig{
		SeedContents: true,
	})
	if err != nil {
		return fmt.Errorf("creating file write: %w", err)
	}
	defer func() {
		if err := writer.Close(); err != nil && returnedErr == nil {
			returnedErr = fmt.Errorf("closing config writer: %w", err)
		}
	}()

	// There is no way to directly unset all keys matching a given regular expression, so we
	// need to go the indirect route and first discover all matching keys via `--get-regex`.
	var stdout bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "config",
		Flags: []git.Option{
			git.Flag{Name: "--name-only"},
			git.Flag{Name: "--get-regex"},
			git.ValueFlag{Name: "--file", Value: writer.Path()},
		},
		Args: []string{regex},
	}, git.WithStdout(&stdout)); err != nil {
		switch {
		case isExitWithCode(err, 1):
			return fmt.Errorf("%w: no matching keys", git.ErrNotFound)
		case isExitWithCode(err, 6):
			// no section or name was provided
			return fmt.Errorf("%w: invalid regular expression", git.ErrInvalidArg)
		}
		return fmt.Errorf("getting matching keys: %w", err)
	}

	keys := strings.Split(text.ChompBytes(stdout.Bytes()), "\n")
	if len(keys) == 0 {
		return fmt.Errorf("%w: no matching keys", git.ErrNotFound)
	}

	keySeen := map[string]bool{}
	for _, key := range keys {
		// We're using `--unset-all`, which will remove each occurrence of the given key
		// even if it's a multi-valued config. We thus have to de-duplicate keys or
		// otherwise we'd try to unset the same key multiple times even though we already
		// removed all entries the first time.
		if keySeen[key] {
			continue
		}
		keySeen[key] = true

		if err := repo.ExecAndWait(ctx, git.SubCmd{
			Name: "config",
			Flags: []git.Option{
				git.Flag{Name: "--unset-all"},
				git.ValueFlag{Name: "--file", Value: writer.Path()},
			},
			Args: []string{key},
		}); err != nil {
			return fmt.Errorf("unsetting key %q: %w", key, err)
		}
	}

	if err := transaction.CommitLockedFile(ctx, txManager, writer); err != nil {
		return fmt.Errorf("committing config: %w", err)
	}

	return nil
}
