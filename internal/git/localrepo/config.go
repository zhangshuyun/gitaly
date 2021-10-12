package localrepo

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
)

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
