// +build static,system_libgit2

package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/git2goutil"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

type rebaseSubcommand struct{}

func (cmd *rebaseSubcommand) Flags() *flag.FlagSet {
	return flag.NewFlagSet("rebase", flag.ExitOnError)
}

func (cmd *rebaseSubcommand) Run(ctx context.Context, r io.Reader, w io.Writer) error {
	var request git2go.RebaseCommand
	if err := gob.NewDecoder(r).Decode(&request); err != nil {
		return err
	}

	commitID, err := cmd.rebase(ctx, &request)
	return gob.NewEncoder(w).Encode(git2go.Result{
		CommitID: commitID,
		Error:    git2go.SerializableError(err),
	})
}

func (cmd *rebaseSubcommand) verify(ctx context.Context, r *git2go.RebaseCommand) error {
	if r.Repository == "" {
		return errors.New("missing repository")
	}
	if r.Committer.Name == "" {
		return errors.New("missing committer name")
	}
	if r.Committer.Email == "" {
		return errors.New("missing committer email")
	}
	if r.BranchName == "" && r.CommitID == "" {
		return errors.New("missing branch name")
	}
	if r.BranchName != "" && r.CommitID != "" {
		return errors.New("both branch name and commit ID")
	}
	if r.UpstreamRevision == "" && r.UpstreamCommitID == "" {
		return errors.New("missing upstream revision")
	}
	if r.UpstreamRevision != "" && r.UpstreamCommitID != "" {
		return errors.New("both upstream revision and upstream commit ID")
	}
	return nil
}

func (cmd *rebaseSubcommand) rebase(ctx context.Context, request *git2go.RebaseCommand) (string, error) {
	if err := cmd.verify(ctx, request); err != nil {
		return "", err
	}

	repo, err := git2goutil.OpenRepository(request.Repository)
	if err != nil {
		return "", fmt.Errorf("open repository: %w", err)
	}

	opts, err := git.DefaultRebaseOptions()
	if err != nil {
		return "", fmt.Errorf("get rebase options: %w", err)
	}
	opts.InMemory = 1

	var commit *git.AnnotatedCommit
	if request.BranchName != "" {
		commit, err = repo.AnnotatedCommitFromRevspec(fmt.Sprintf("refs/heads/%s", request.BranchName))
		if err != nil {
			return "", fmt.Errorf("look up branch %q: %w", request.BranchName, err)
		}
	} else {
		commitOid, err := git.NewOid(request.CommitID.String())
		if err != nil {
			return "", fmt.Errorf("parse commit %q: %w", request.CommitID, err)
		}

		commit, err = repo.LookupAnnotatedCommit(commitOid)
		if err != nil {
			return "", fmt.Errorf("look up commit %q: %w", request.CommitID, err)
		}
	}

	upstreamCommitParam := request.UpstreamRevision
	if upstreamCommitParam == "" {
		upstreamCommitParam = request.UpstreamCommitID.String()
	}

	upstreamCommitOID, err := git.NewOid(upstreamCommitParam)
	if err != nil {
		return "", fmt.Errorf("parse upstream revision %q: %w", upstreamCommitParam, err)
	}

	upstreamCommit, err := repo.LookupAnnotatedCommit(upstreamCommitOID)
	if err != nil {
		return "", fmt.Errorf("look up upstream revision %q: %w", upstreamCommitParam, err)
	}

	mergeBase, err := repo.MergeBase(upstreamCommit.Id(), commit.Id())
	if err != nil {
		return "", fmt.Errorf("find merge base: %w", err)
	}

	if mergeBase.Equal(upstreamCommit.Id()) {
		// Branch is zero commits behind, so do not rebase
		return commit.Id().String(), nil
	}

	if mergeBase.Equal(commit.Id()) {
		// Branch is merged, so fast-forward to upstream
		return upstreamCommit.Id().String(), nil
	}

	mergeCommit, err := repo.LookupAnnotatedCommit(mergeBase)
	if err != nil {
		return "", fmt.Errorf("look up merge base: %w", err)
	}

	rebase, err := repo.InitRebase(commit, mergeCommit, upstreamCommit, &opts)
	if err != nil {
		return "", fmt.Errorf("initiate rebase: %w", err)
	}

	committer := git.Signature(request.Committer)
	var oid *git.Oid
	for {
		op, err := rebase.Next()
		if git.IsErrorCode(err, git.ErrIterOver) {
			break
		} else if err != nil {
			return "", fmt.Errorf("rebase iterate: %w", err)
		}

		commit, err := repo.LookupCommit(op.Id)
		if err != nil {
			return "", fmt.Errorf("lookup commit: %w", err)
		}

		oid = op.Id.Copy()
		err = rebase.Commit(oid, nil, &committer, commit.Message())
		if err != nil {
			return "", fmt.Errorf("commit %q: %w", op.Id.String(), err)
		}
	}

	if oid == nil {
		return commit.Id().String(), nil
	}

	if err = rebase.Finish(); err != nil {
		return "", fmt.Errorf("finish rebase: %w", err)
	}

	return oid.String(), nil
}
