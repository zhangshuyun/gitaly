// +build static,system_libgit2

package commit

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	git "github.com/libgit2/git2go/v30"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
)

// Run runs the commit subcommand.
func Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	var params git2go.CommitParams
	if err := gob.NewDecoder(stdin).Decode(&params); err != nil {
		return err
	}

	commitID, err := commit(ctx, params)
	return gob.NewEncoder(stdout).Encode(git2go.Result{
		CommitID: commitID,
		Error:    git2go.SerializableError(err),
	})
}

func commit(ctx context.Context, params git2go.CommitParams) (string, error) {
	repo, err := git.OpenRepository(params.Repository)
	if err != nil {
		return "", fmt.Errorf("open repository: %w", err)
	}

	index, err := git.NewIndex()
	if err != nil {
		return "", fmt.Errorf("new index: %w", err)
	}

	var parents []*git.Oid
	if params.Parent != "" {
		parentOID, err := git.NewOid(params.Parent)
		if err != nil {
			return "", fmt.Errorf("parse base commit oid: %w", err)
		}

		parents = []*git.Oid{parentOID}

		baseCommit, err := repo.LookupCommit(parentOID)
		if err != nil {
			return "", fmt.Errorf("lookup commit: %w", err)
		}

		baseTree, err := baseCommit.Tree()
		if err != nil {
			return "", fmt.Errorf("lookup tree: %w", err)
		}

		if err := index.ReadTree(baseTree); err != nil {
			return "", fmt.Errorf("read tree: %w", err)
		}
	}

	for _, action := range params.Actions {
		if err := apply(action, repo, index); err != nil {
			return "", fmt.Errorf("apply action %T: %w", action, err)
		}
	}

	treeOID, err := index.WriteTreeTo(repo)
	if err != nil {
		return "", fmt.Errorf("write tree: %w", err)
	}

	author := git.Signature(params.Author)
	committer := git.Signature(params.Committer)
	commitID, err := repo.CreateCommitFromIds("", &author, &committer, params.Message, treeOID, parents...)
	if err != nil {
		if git.IsErrorClass(err, git.ErrClassInvalid) {
			return "", git2go.InvalidArgumentError(err.Error())
		}

		return "", fmt.Errorf("create commit: %w", err)
	}

	return commitID.String(), nil
}

func apply(action git2go.Action, repo *git.Repository, index *git.Index) error {
	err := applyHelper(action, repo, index)

	if git.IsErrorClass(err, git.ErrClassIndex) {
		return git2go.IndexError(err.Error())
	}

	return err
}

func applyHelper(action git2go.Action, repo *git.Repository, index *git.Index) error {
	switch action := action.(type) {
	case git2go.ChangeFileMode:
		return applyChangeFileMode(action, index)
	case git2go.CreateDirectory:
		return applyCreateDirectory(action, repo, index)
	case git2go.CreateFile:
		return applyCreateFile(action, index)
	case git2go.DeleteFile:
		return applyDeleteFile(action, index)
	case git2go.MoveFile:
		return applyMoveFile(action, index)
	case git2go.UpdateFile:
		return applyUpdateFile(action, index)
	default:
		return errors.New("unsupported action")
	}
}
