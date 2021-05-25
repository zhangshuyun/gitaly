// +build static,system_libgit2

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

type patchIterator struct {
	value   git2go.Patch
	decoder *gob.Decoder
	error   error
}

func (iter *patchIterator) Next() bool {
	if err := iter.decoder.Decode(&iter.value); err != nil {
		if !errors.Is(err, io.EOF) {
			iter.error = fmt.Errorf("decode patch: %w", err)
		}

		return false
	}

	return true
}

func (iter *patchIterator) Value() git2go.Patch { return iter.value }

func (iter *patchIterator) Err() error { return iter.error }

type applySubcommand struct {
	gitBinaryPath string
}

func (cmd *applySubcommand) Flags() *flag.FlagSet {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	fs.StringVar(&cmd.gitBinaryPath, "git-binary-path", "", "Path to the Git binary.")
	return fs
}

// Run runs the subcommand.
func (cmd *applySubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	decoder := gob.NewDecoder(stdin)

	var params git2go.ApplyParams
	if err := decoder.Decode(&params); err != nil {
		return fmt.Errorf("decode params: %w", err)
	}

	params.Patches = &patchIterator{decoder: decoder}
	commitID, err := cmd.apply(ctx, params)
	return gob.NewEncoder(stdout).Encode(git2go.Result{
		CommitID: commitID,
		Error:    git2go.SerializableError(err),
	})
}

func (cmd *applySubcommand) apply(ctx context.Context, params git2go.ApplyParams) (string, error) {
	repo, err := git.OpenRepository(params.Repository)
	if err != nil {
		return "", fmt.Errorf("open repository: %w", err)
	}

	commitOID, err := git.NewOid(params.ParentCommit)
	if err != nil {
		return "", fmt.Errorf("parse parent commit oid: %w", err)
	}

	committer := git.Signature(params.Committer)
	for i := 0; params.Patches.Next(); i++ {
		commitOID, err = cmd.applyPatch(ctx, repo, &committer, commitOID, params.Patches.Value())
		if err != nil {
			return "", fmt.Errorf("apply patch [%d]: %w", i+1, err)
		}
	}

	if err := params.Patches.Err(); err != nil {
		return "", fmt.Errorf("reading patches: %w", err)
	}

	return commitOID.String(), nil
}

func (cmd *applySubcommand) applyPatch(
	ctx context.Context,
	repo *git.Repository,
	committer *git.Signature,
	parentCommitOID *git.Oid,
	patch git2go.Patch,
) (*git.Oid, error) {
	parentCommit, err := repo.LookupCommit(parentCommitOID)
	if err != nil {
		return nil, fmt.Errorf("lookup commit: %w", err)
	}

	parentTree, err := parentCommit.Tree()
	if err != nil {
		return nil, fmt.Errorf("lookup tree: %w", err)
	}

	diff, err := git.DiffFromBuffer(patch.Diff, repo)
	if err != nil {
		return nil, fmt.Errorf("diff from buffer: %w", err)
	}

	patchedIndex, err := repo.ApplyToTree(diff, parentTree, nil)
	if err != nil {
		if !git.IsErrorCode(err, git.ErrApplyFail) {
			return nil, fmt.Errorf("apply to tree: %w", err)
		}

		patchedIndex, err = cmd.threeWayMerge(ctx, repo, parentTree, diff, patch.Diff)
		if err != nil {
			return nil, fmt.Errorf("three way merge: %w", err)
		}
	}

	patchedTree, err := patchedIndex.WriteTreeTo(repo)
	if err != nil {
		return nil, fmt.Errorf("write patched tree: %w", err)
	}

	author := git.Signature(patch.Author)
	patchedCommitOID, err := repo.CreateCommitFromIds("", &author, committer, patch.Message, patchedTree, parentCommitOID)
	if err != nil {
		return nil, fmt.Errorf("create commit: %w", err)
	}

	return patchedCommitOID, nil
}

// threeWayMerge attempts a three-way merge as a fallback if applying the patch fails.
// Fallback three-way merge is only possible if the patch records the pre-image blobs
// and the repository contains them. It works as follows:
//
// 1. An index that contains only the pre-image blobs of the patch is built. This is done
//    by calling `git apply --build-fake-ancestor`. The tree of the index is the fake
//    ancestor tree.
// 2. The fake ancestor tree is patched to produce the post-image tree of the patch.
// 3. Three-way merge is performed with fake ancestor tree as the common ancestor, the
//    base commit's tree as our tree and the patched fake ancestor tree as their tree.
func (cmd *applySubcommand) threeWayMerge(
	ctx context.Context,
	repo *git.Repository,
	our *git.Tree,
	diff *git.Diff,
	rawDiff []byte,
) (*git.Index, error) {
	ancestorTree, err := cmd.buildFakeAncestor(ctx, repo, rawDiff)
	if err != nil {
		return nil, fmt.Errorf("build fake ancestor: %w", err)
	}

	patchedAncestorIndex, err := repo.ApplyToTree(diff, ancestorTree, nil)
	if err != nil {
		return nil, fmt.Errorf("patch fake ancestor: %w", err)
	}

	patchedAncestorTreeOID, err := patchedAncestorIndex.WriteTreeTo(repo)
	if err != nil {
		return nil, fmt.Errorf("write patched fake ancestor: %w", err)
	}

	patchedTree, err := repo.LookupTree(patchedAncestorTreeOID)
	if err != nil {
		return nil, fmt.Errorf("lookup patched tree: %w", err)
	}

	patchedIndex, err := repo.MergeTrees(ancestorTree, our, patchedTree, nil)
	if err != nil {
		return nil, fmt.Errorf("merge trees: %w", err)
	}

	if patchedIndex.HasConflicts() {
		return nil, git2go.ErrMergeConflict
	}

	return patchedIndex, nil
}

func (cmd *applySubcommand) buildFakeAncestor(ctx context.Context, repo *git.Repository, diff []byte) (*git.Tree, error) {
	dir, err := ioutil.TempDir("", "gitaly-git2go")
	if err != nil {
		return nil, fmt.Errorf("create temporary directory: %w", err)
	}
	defer os.RemoveAll(dir)

	file := filepath.Join(dir, "patch-merge-index")
	gitCmd := exec.CommandContext(ctx, cmd.gitBinaryPath, "--git-dir", repo.Path(), "apply", "--build-fake-ancestor", file)
	gitCmd.Stdin = bytes.NewReader(diff)
	if _, err := gitCmd.Output(); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			err = fmt.Errorf("%w, stderr: %q", err, exitError.Stderr)
		}

		return nil, fmt.Errorf("git: %w", err)
	}

	fakeAncestor, err := git.OpenIndex(file)
	if err != nil {
		return nil, fmt.Errorf("open fake ancestor index: %w", err)
	}

	ancestorTreeOID, err := fakeAncestor.WriteTreeTo(repo)
	if err != nil {
		return nil, fmt.Errorf("write fake ancestor tree: %w", err)
	}

	return repo.LookupTree(ancestorTreeOID)
}
