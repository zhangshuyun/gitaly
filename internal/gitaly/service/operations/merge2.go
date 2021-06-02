package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func validateMergeBranchRequest2(request *gitalypb.UserMergeBranchRequest) error {
	if request.User == nil {
		return fmt.Errorf("empty user")
	}

	if len(request.Branch) == 0 {
		return fmt.Errorf("empty branch name")
	}

	if request.CommitId == "" {
		return fmt.Errorf("empty commit ID")
	}

	if len(request.Message) == 0 {
		return fmt.Errorf("empty message")
	}

	return nil
}

func (s *Server) UserMergeBranch2(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest2(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := firstRequest.Repository
	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return err
	}

	referenceName := git.NewReferenceNameFromBranchName(string(firstRequest.Branch))

	revision, err := s.localrepo(repo).ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return err
	}

	env := alternates.Env(repoPath, repo.GetGitObjectDirectory(), repo.GetGitAlternateObjectDirectories())

	err = s.userMerge(ctx, referenceName, firstRequest.CommitId, env, repo, repoPath)
	if err != nil {
		var gitErr gitError2
		if errors.As(err, &gitErr) {
			if gitErr.ErrMsg != "" {
				// we log an actual error as it would be lost otherwise (it is not sent back to the client)
				ctxlogrus.Extract(ctx).WithError(err).Error("user merge")
				return helper.ErrInternalf("Git error: %w", gitErr)
			}
		}

		return helper.ErrInternal(err)
	}
	
	mergeOID, err := s.localrepo(repo).ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return helper.ErrInternalf("could not resolve merge ID: %w", err)
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		CommitId: mergeOID.String(),
	}); err != nil {
		return err
	}

	secondRequest, err := stream.Recv()
	if err != nil {
		return err
	}
	if !secondRequest.Apply {
		return helper.ErrPreconditionFailedf("merge aborted by client")
	}

	if err := s.updateReferenceWithHooks(ctx, firstRequest.Repository, firstRequest.User, referenceName, mergeOID, revision); err != nil {
		var preReceiveError preReceiveError
		var updateRefError updateRefError

		if errors.As(err, &preReceiveError) {
			err = stream.Send(&gitalypb.UserMergeBranchResponse{
				PreReceiveError: preReceiveError.message,
			})
		} else if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			err = stream.Send(&gitalypb.UserMergeBranchResponse{})
		}

		return err
	}

	if err := stream.Send(&gitalypb.UserMergeBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      mergeOID.String(),
			RepoCreated:   false,
			BranchCreated: false,
		},
	}); err != nil {
		return err
	}

	return nil
}

const (
	gitlabWorktreesSubDir2 = "gitlab-worktree"
)

func newWorktreePath(repoPath, prefix, id string) string {
	suffix := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(suffix), func(i, j int) { suffix[i], suffix[j] = suffix[j], suffix[i] })

	worktreeName := prefix + "-" + id + "-" + string(suffix[:32])
	return filepath.Join(repoPath, gitlabWorktreesSubDir2, worktreeName)
}

func (s *Server) addWorktree2(ctx context.Context, repo *gitalypb.Repository, worktreePath string, branch git.ReferenceName) error {
	if err := s.runCmd2(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.splitIndex", Value: "false"}}, nil); err != nil {
		return fmt.Errorf("on 'git config core.splitIndex false': %w", err)
	}

	args := []string{worktreePath, branch.String()}

	var stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "worktree",
			Action: "add",
			Args:   args,
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("creation of 'git worktree add': %w", gitError2{ErrMsg: stderr.String(), Err: err})
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'git worktree add': %w", gitError2{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) checkout2(ctx context.Context, repo *gitalypb.Repository, worktreePath string, branch git.ReferenceName) error {
	var stderr bytes.Buffer
	checkoutCmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name:  "checkout",
			Args:  []string{branch.String()},
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("create 'git checkout': %w", gitError2{ErrMsg: stderr.String(), Err: err})
	}

	if err = checkoutCmd.Wait(); err != nil {
		if strings.Contains(stderr.String(), "error: Sparse checkout leaves no entry on working directory") {
			return errNoFilesCheckedOut
		}

		return fmt.Errorf("wait for 'git checkout': %w", gitError2{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) userMerge(ctx context.Context, referenceName git.ReferenceName, commitID string, env []string,
	repo *gitalypb.Repository, repoPath string) (error) {

	worktreePath := newWorktreePath(repoPath, "merge", commitID)

	if err := s.addWorktree2(ctx, repo, worktreePath, referenceName); err != nil {
		return fmt.Errorf("add worktree: %w", err)
	}

	defer func(worktreeName string) {
		ctx, cancel := context.WithCancel(helper.SuppressCancellation(ctx))
		defer cancel()

		if err := s.removeWorktree2(ctx, repo, worktreeName); err != nil {
			ctxlogrus.Extract(ctx).WithField("worktree_name", worktreeName).WithError(err).Error("failed to remove worktree")
		}
	}(filepath.Base(worktreePath))

	worktreeGitPath, err := s.revParseGitDir(ctx, worktreePath)
	if err != nil {
		return fmt.Errorf("define git dir for worktree: %w", err)
	}

	if err := s.runCmd2(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.sparseCheckout", Value: "true"}}, nil); err != nil {
		return fmt.Errorf("on 'git config core.sparseCheckout true': %w", err)
	}

	sparseDiffFiles, err := s.diffFiles2(ctx, env, repoPath, referenceName, commitID)
	if err != nil {
		return fmt.Errorf("define diff files: %w", err)
	}

	if err := s.createSparseCheckoutFile2(worktreeGitPath, sparseDiffFiles); err != nil {
		return fmt.Errorf("create sparse checkout file: %w", err)
	}

	if err := s.checkout2(ctx, repo, worktreePath, referenceName); err != nil {
		if !errors.Is(err, errNoFilesCheckedOut) {
			return fmt.Errorf("perform 'git checkout' with core.sparseCheckout true: %w", err)
		}

		// try to perform checkout with disabled sparseCheckout feature
		if err := s.runCmd2(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.sparseCheckout", Value: "false"}}, nil); err != nil {
			return fmt.Errorf("on 'git config core.sparseCheckout false': %w", err)
		}

		if err := s.checkout2(ctx, repo, worktreePath, referenceName); err != nil {
			return fmt.Errorf("perform 'git checkout' with core.sparseCheckout false: %w", err)
		}
	}

	var mergeStderr bytes.Buffer
	cmdMerge, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name: "merge",
			Flags: []git.Option{
				git.Flag{Name: "--strategy=ort"},
			},
			Args: []string{commitID},
		},
		git.WithEnv(env...),
		git.WithStderr(&mergeStderr),
	)
	if err != nil {
		return fmt.Errorf("merge for branch: %w", gitError2{ErrMsg: mergeStderr.String(), Err: err})
	}

	if err := cmdMerge.Wait(); err != nil {
		return fmt.Errorf("wait for 'git merge': %w", gitError{ErrMsg: mergeStderr.String(), Err: err})
	}

	return nil
}

type gitError2 struct {
	// ErrMsg error message from 'git' executable if any.
	ErrMsg string
	// Err is an error that happened during rebase process.
	Err error
}

func (er gitError2) Error() string {
	return er.ErrMsg + ": " + er.Err.Error()
}

func (s *Server) removeWorktree2(ctx context.Context, repo *gitalypb.Repository, worktreeName string) error {
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "worktree",
			Action: "remove",
			Flags:  []git.Option{git.Flag{Name: "--force"}},
			Args:   []string{worktreeName},
		},
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("creation of 'worktree remove': %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'worktree remove': %w", err)
	}

	return nil
}

func (s *Server) createSparseCheckoutFile2(worktreeGitPath string, diffFilesOut []byte) error {
	if err := os.MkdirAll(filepath.Join(worktreeGitPath, "info"), 0755); err != nil {
		return fmt.Errorf("create 'info' dir for worktree %q: %w", worktreeGitPath, err)
	}

	if err := ioutil.WriteFile(filepath.Join(worktreeGitPath, "info", "sparse-checkout"), diffFilesOut, 0666); err != nil {
		return fmt.Errorf("create 'sparse-checkout' file for worktree %q: %w", worktreeGitPath, err)
	}

	return nil
}

func (s *Server) runCmd2(ctx context.Context, repo *gitalypb.Repository, cmd string, opts []git.Option, args []string) error {
	var stderr bytes.Buffer
	safeCmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{Name: cmd, Flags: opts, Args: args}, git.WithStderr(&stderr))
	if err != nil {
		return fmt.Errorf("create safe cmd %q: %w", cmd, gitError2{ErrMsg: stderr.String(), Err: err})
	}

	if err := safeCmd.Wait(); err != nil {
		return fmt.Errorf("wait safe cmd %q: %w", cmd, gitError2{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) diffFiles2(ctx context.Context, env []string, repoPath string,
	referenceName git.ReferenceName, commitID string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.NewWithDir(ctx, repoPath,
		git.SubCmd{
			Name:  "diff",
			Flags: []git.Option{git.Flag{Name: "--name-only"}, git.Flag{Name: "--diff-filter=ar"}, git.Flag{Name: "--binary"}},
			Args:  []string{diffRange2(referenceName, commitID)},
		},
		git.WithEnv(env...),
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, fmt.Errorf("create 'git diff': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("on 'git diff' awaiting: %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return stdout.Bytes(), nil
}

func diffRange2(referenceName git.ReferenceName, commitID string) string {
	return referenceName.String() + "..." + commitID
}

