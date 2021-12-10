package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const attributesFileMode os.FileMode = 0o644

func (s *server) applyGitattributes(ctx context.Context, repo *localrepo.Repo, objectReader catfile.ObjectReader, repoPath string, revision []byte) error {
	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	_, err := repo.ResolveRevision(ctx, git.Revision(revision)+"^{commit}")
	if err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return helper.ErrInvalidArgumentf("revision does not exist")
		}

		return err
	}

	blobObj, err := objectReader.Object(ctx, git.Revision(fmt.Sprintf("%s:.gitattributes", revision)))
	if err != nil && !catfile.IsNotFound(err) {
		return err
	}

	// Create  /info folder if it doesn't exist
	if err := os.MkdirAll(infoPath, 0o755); err != nil {
		return err
	}

	if catfile.IsNotFound(err) || blobObj.Type != "blob" {
		if featureflag.TxApplyGitattributesTwoPhaseRemoval.IsEnabled(ctx) {
			locker, err := safe.NewLockingFileWriter(attributesPath, safe.LockingFileWriterConfig{
				FileWriterConfig: safe.FileWriterConfig{FileMode: attributesFileMode},
			})
			if err != nil {
				return fmt.Errorf("creating gitattributes lock: %w", err)
			}
			defer func() {
				if err := locker.Close(); err != nil {
					ctxlogrus.Extract(ctx).WithError(err).Error("unlocking gitattributes")
				}
			}()

			if err := locker.Lock(); err != nil {
				return fmt.Errorf("locking gitattributes: %w", err)
			}

			// We use the zero OID as placeholder to vote on removal of the
			// gitattributes file.
			if err := s.vote(ctx, git.ZeroOID); err != nil {
				return fmt.Errorf("preimage vote: %w", err)
			}

			if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
				return err
			}

			if err := s.vote(ctx, git.ZeroOID); err != nil {
				return fmt.Errorf("postimage vote: %w", err)
			}

			return nil
		}

		// If there is no gitattributes file, we simply use the ZeroOID
		// as a placeholder to vote on the removal.
		if err := s.vote(ctx, git.ZeroOID); err != nil {
			return fmt.Errorf("could not remove gitattributes: %w", err)
		}

		// Remove info/attributes file if there's no .gitattributes file
		if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
			return err
		}

		return nil
	}

	writer, err := safe.NewLockingFileWriter(attributesPath, safe.LockingFileWriterConfig{
		FileWriterConfig: safe.FileWriterConfig{FileMode: attributesFileMode},
	})
	if err != nil {
		return fmt.Errorf("creating gitattributes writer: %w", err)
	}
	defer writer.Close()

	if _, err := io.Copy(writer, blobObj); err != nil {
		return err
	}

	if err := transaction.CommitLockedFile(ctx, s.txManager, writer); err != nil {
		return fmt.Errorf("committing gitattributes: %w", err)
	}

	return nil
}

func (s *server) vote(ctx context.Context, oid git.ObjectID) error {
	tx, err := txinfo.TransactionFromContext(ctx)
	if errors.Is(err, txinfo.ErrTransactionNotFound) {
		return nil
	}

	hash, err := oid.Bytes()
	if err != nil {
		return fmt.Errorf("vote with invalid object ID: %w", err)
	}

	vote, err := voting.VoteFromHash(hash)
	if err != nil {
		return fmt.Errorf("cannot convert OID to vote: %w", err)
	}

	if err := s.txManager.Vote(ctx, tx, vote); err != nil {
		return fmt.Errorf("vote failed: %w", err)
	}

	return nil
}

func (s *server) ApplyGitattributes(ctx context.Context, in *gitalypb.ApplyGitattributesRequest) (*gitalypb.ApplyGitattributesResponse, error) {
	repo := s.localrepo(in.GetRepository())
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return nil, helper.ErrInvalidArgumentf("revision: %v", err)
	}

	objectReader, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return nil, err
	}

	if err := s.applyGitattributes(ctx, repo, objectReader, repoPath, in.GetRevision()); err != nil {
		return nil, err
	}

	return &gitalypb.ApplyGitattributesResponse{}, nil
}
