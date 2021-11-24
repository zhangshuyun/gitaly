package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) removeOriginInRepo(ctx context.Context, repository *gitalypb.Repository) error {
	cmd, err := s.gitCmdFactory.New(ctx, repository, git.SubCmd{Name: "remote", Args: []string{"remove", "origin"}}, git.WithRefTxHook(ctx, repository, s.cfg))
	if err != nil {
		return fmt.Errorf("remote cmd start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("remote cmd wait: %v", err)
	}

	return nil
}

// createRepository will create a new repository in a race-free way with proper transactional
// semantics. The repository will only be created if it doesn't yet exist and if nodes which take
// part in the transaction reach quorum. Otherwise, the target path of the new repository will not
// be modified. The repository can optionally be seeded with contents
func (s *server) createRepository(
	ctx context.Context,
	repository *gitalypb.Repository,
	seedRepository func(repository *gitalypb.Repository) error,
) error {
	targetPath, err := s.locator.GetPath(repository)
	if err != nil {
		return helper.ErrInvalidArgumentf("locate repository: %w", err)
	}

	// The repository must not exist on disk already, or otherwise we won't be able to
	// create it with atomic semantics.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return helper.ErrAlreadyExistsf("repository exists already")
	}

	// Create the parent directory in case it doesn't exist yet.
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o770); err != nil {
		return helper.ErrInternalf("create directories: %w", err)
	}

	newRepo, newRepoDir, err := tempdir.NewRepository(ctx, repository.GetStorageName(), s.locator)
	if err != nil {
		return fmt.Errorf("creating temporary repository: %w", err)
	}
	defer func() {
		// We don't really care about whether this succeeds or not. It will either get
		// cleaned up after the context is done, or eventually by the tempdir walker when
		// it's old enough.
		_ = os.RemoveAll(newRepoDir.Path())
	}()

	// Note that we do not create the repository directly in its target location, but
	// instead create it in a temporary directory, first. This is done such that we can
	// guarantee atomicity and roll back the change easily in case an error happens.
	stderr := &bytes.Buffer{}
	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
		Name: "init",
		Flags: []git.Option{
			git.Flag{Name: "--bare"},
			git.Flag{Name: "--quiet"},
		},
		Args: []string{newRepoDir.Path()},
	}, git.WithStderr(stderr))
	if err != nil {
		return fmt.Errorf("spawning git-init: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("creating repository: %w, stderr: %q", err, stderr.String())
	}

	if err := seedRepository(newRepo); err != nil {
		// Return the error returned by the callback function as-is so we don't clobber any
		// potential returned gRPC error codes.
		return err
	}

	// In order to guarantee that the repository is going to be the same across all
	// Gitalies in case we're behind Praefect, we walk the repository and hash all of
	// its files.
	voteHash := voting.NewVoteHash()
	if err := filepath.WalkDir(newRepoDir.Path(), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// The way packfiles are generated may not be deterministic, so we skip over the
		// object database.
		if path == filepath.Join(newRepoDir.Path(), "objects") {
			return fs.SkipDir
		}

		// We do not care about directories.
		if entry.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %q: %w", entry.Name(), err)
		}
		defer file.Close()

		if _, err := io.Copy(voteHash, file); err != nil {
			return fmt.Errorf("hashing %q: %w", entry.Name(), err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("walking repository: %w", err)
	}

	vote, err := voteHash.Vote()
	if err != nil {
		return fmt.Errorf("computing vote: %w", err)
	}

	// We're somewhat abusing this file writer given that we simply want to assert that
	// the target directory doesn't exist and isn't created while we want to move the
	// new repository into place. We thus only use the locking semantics of the writer,
	// but will never commit it.
	locker, err := safe.NewLockingFileWriter(targetPath)
	if err != nil {
		return fmt.Errorf("creating locker: %w", err)
	}
	defer func() {
		if err := locker.Close(); err != nil {
			ctxlogrus.Extract(ctx).Error("closing repository locker: %w", err)
		}
	}()

	// We're now entering the critical section where we want to have exclusive access
	// over creation of the repository. So we:
	//
	// 1. Lock the repository path such that no other process can create it at the same
	//    time.
	// 2. Vote on the new repository's state.
	// 3. Move the repository into place.
	// 4. Do another confirmatory vote to signal that we performed the change.
	// 5. Unlock the repository again.
	//
	// This sequence guarantees that the change is atomic and can trivially be rolled
	// back in case we fail to either lock the repository or reach quorum in the initial
	// vote.
	if err := locker.Lock(); err != nil {
		return fmt.Errorf("locking repository: %w", err)
	}

	// Now that the repository is locked, we must assert that it _still_ doesn't exist.
	// Otherwise, it could have happened that a concurrent RPC call created it while we created
	// and seeded our temporary repository. While we would notice this at the point of moving
	// the repository into place, we want to be as sure as possible that the action will succeed
	// previous to the first transactional vote.
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		return helper.ErrAlreadyExistsf("repository exists already")
	}

	if err := transaction.VoteOnContext(ctx, s.txManager, vote); err != nil {
		return helper.ErrFailedPreconditionf("preparatory vote: %w", err)
	}

	// Now that we have locked the repository and all Gitalies have agreed that they
	// want to do the same change we can move the repository into place.
	if err := os.Rename(newRepoDir.Path(), targetPath); err != nil {
		return fmt.Errorf("moving repository into place: %w", err)
	}

	if err := transaction.VoteOnContext(ctx, s.txManager, vote); err != nil {
		return helper.ErrFailedPreconditionf("committing vote: %w", err)
	}

	// We unlock the repository implicitly via the deferred `Close()` call.
	return nil
}
