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
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func validateMergeBranchRequest(request *gitalypb.UserMergeBranchRequest) error {
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

// UserMergeBranch is using the new merge-ort merge strategy. This is
// a test to see how it goes and what is needed to make that work
// wihtout a worktree.
func (s *Server) UserMergeBranch(stream gitalypb.OperationService_UserMergeBranchServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateMergeBranchRequest(firstRequest); err != nil {
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

	err = s.userMerge(ctx, referenceName, firstRequest, env, repo, repoPath)
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
			Name: "checkout",
			Args: []string{branch.String()},
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

func (s *Server) userMerge(ctx context.Context, referenceName git.ReferenceName, firstRequest *gitalypb.UserMergeBranchRequest,
	env []string, repo *gitalypb.Repository, repoPath string) error {
	commitID := firstRequest.CommitId
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

	// 	Merge params:
	//               AuthorName: string(firstRequest.User.Name),
	//               AuthorMail: string(firstRequest.User.Email),
	//               AuthorDate: authorDate,
	//               Message:    string(firstRequest.Message),
	//               Ours:       revision.String(),
	//               Theirs:     firstRequest.CommitId

	env = append(env, fmt.Sprintf("GIT_AUTHOR_NAME=%s", string(firstRequest.User.Name)))
	env = append(env, fmt.Sprintf("GIT_AUTHOR_EMAIL=%s", string(firstRequest.User.Email)))

	authorDate := time.Now()
	if firstRequest.Timestamp != nil {
		authorDate, err = ptypes.Timestamp(firstRequest.Timestamp)
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}
	}
	env = append(env, fmt.Sprintf("GIT_AUTHOR_DATE=%s", authorDate))

	var mergeStderr bytes.Buffer
	cmdMerge, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name: "merge",
			Flags: []git.Option{
				git.Flag{Name: "--strategy=ort"},
				git.ValueFlag{Name: "-m", Value: string(firstRequest.Message)},
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

func validateFFRequest(in *gitalypb.UserFFBranchRequest) error {
	if len(in.Branch) == 0 {
		return fmt.Errorf("empty branch name")
	}

	if in.User == nil {
		return fmt.Errorf("empty user")
	}

	if in.CommitId == "" {
		return fmt.Errorf("empty commit id")
	}

	return nil
}

func (s *Server) UserFFBranch(ctx context.Context, in *gitalypb.UserFFBranchRequest) (*gitalypb.UserFFBranchResponse, error) {
	if err := validateFFRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	referenceName := git.NewReferenceNameFromBranchName(string(in.Branch))

	repo := s.localrepo(in.GetRepository())
	revision, err := repo.ResolveRevision(ctx, referenceName.Revision())
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	commitID, err := git.NewObjectIDFromHex(in.CommitId)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("cannot parse commit ID: %w", err)
	}

	ancestor, err := repo.IsAncestor(ctx, revision.Revision(), commitID.Revision())
	if err != nil {
		return nil, err
	}
	if !ancestor {
		return nil, helper.ErrPreconditionFailedf("not fast forward")
	}

	if err := s.updateReferenceWithHooks(ctx, in.Repository, in.User, referenceName, commitID, revision); err != nil {
		var preReceiveError preReceiveError
		if errors.As(err, &preReceiveError) {
			return &gitalypb.UserFFBranchResponse{
				PreReceiveError: preReceiveError.message,
			}, nil
		}

		var updateRefError updateRefError
		if errors.As(err, &updateRefError) {
			// When an error happens updating the reference, e.g. because of a race
			// with another update, then Ruby code didn't send an error but just an
			// empty response.
			return &gitalypb.UserFFBranchResponse{}, nil
		}

		return nil, err
	}

	return &gitalypb.UserFFBranchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId: in.CommitId,
		},
	}, nil
}

func validateUserMergeToRefRequest(in *gitalypb.UserMergeToRefRequest) error {
	if len(in.FirstParentRef) == 0 && len(in.Branch) == 0 {
		return fmt.Errorf("empty first parent ref and branch name")
	}

	if in.User == nil {
		return fmt.Errorf("empty user")
	}

	if in.SourceSha == "" {
		return fmt.Errorf("empty source SHA")
	}

	if len(in.TargetRef) == 0 {
		return fmt.Errorf("empty target ref")
	}

	if !strings.HasPrefix(string(in.TargetRef), "refs/merge-requests") {
		return fmt.Errorf("invalid target ref")
	}

	return nil
}

// UserMergeToRef overwrites the given TargetRef to point to either Branch or
// FirstParentRef. Afterwards, it performs a merge of SourceSHA with either
// Branch or FirstParentRef and updates TargetRef to the merge commit.
func (s *Server) UserMergeToRef(ctx context.Context, request *gitalypb.UserMergeToRefRequest) (*gitalypb.UserMergeToRefResponse, error) {
	if err := validateUserMergeToRefRequest(request); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return nil, err
	}

	repo := s.localrepo(request.GetRepository())

	revision := git.Revision(request.Branch)
	if request.FirstParentRef != nil {
		revision = git.Revision(request.FirstParentRef)
	}

	oid, err := repo.ResolveRevision(ctx, revision)
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(request.SourceSha))
	if err != nil {
		//nolint:stylecheck
		return nil, helper.ErrInvalidArgument(errors.New("Invalid merge source"))
	}

	authorDate := time.Now()
	if request.Timestamp != nil {
		authorDate, err = ptypes.Timestamp(request.Timestamp)
		if err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}
	}

	var oldTargetOID git.ObjectID
	if featureflag.IsEnabled(ctx, featureflag.UserMergeToRefSkipPrecursorRefUpdate) {
		// Resolve the current state of the target reference. We do not care whether it
		// exists or not, but what we do want to assert is that the target reference doesn't
		// change while we compute the merge commit as a small protection against races.
		targetRef, err := repo.GetReference(ctx, git.ReferenceName(request.TargetRef))
		if err == nil {
			if targetRef.IsSymbolic {
				return nil, helper.ErrPreconditionFailedf("target reference is symbolic: %q", request.TargetRef)
			}

			oid, err := git.NewObjectIDFromHex(targetRef.Target)
			if err != nil {
				return nil, helper.ErrInternalf("invalid target revision: %v", err)
			}

			oldTargetOID = oid
		} else if errors.Is(err, git.ErrReferenceNotFound) {
			oldTargetOID = git.ZeroOID
		} else {
			return nil, helper.ErrInternalf("could not read target reference: %v", err)
		}
	} else {
		// This is the old code path which always force-updated the target reference before
		// computing the merge. As a result, even if the merge failed, we'd have updated the
		// reference to point to the first parent ref. It does feel unexpected that the
		// target reference changes even if the merge itself fails. Furthermore, this is
		// causing problems with transactions: if the merge fails, we have already voted
		// once to update the target reference and will thus trigger a replication job.
		if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), oid, ""); err != nil {
			return nil, updateRefError{reference: string(request.TargetRef)}
		}
		oldTargetOID = oid
	}

	// Now, we create the merge commit...
	merge, err := git2go.MergeCommand{
		Repository:     repoPath,
		AuthorName:     string(request.User.Name),
		AuthorMail:     string(request.User.Email),
		AuthorDate:     authorDate,
		Message:        string(request.Message),
		Ours:           oid.String(),
		Theirs:         sourceOID.String(),
		AllowConflicts: request.AllowConflicts,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrPreconditionFailedf("Failed to create merge commit for source_sha %s and target_sha %s at %s",
			sourceOID, oid, string(request.TargetRef))
	}

	mergeOID, err := git.NewObjectIDFromHex(merge.CommitID)
	if err != nil {
		return nil, err
	}

	// ... and move branch from target ref to the merge commit. The Ruby
	// implementation doesn't invoke hooks, so we don't either.
	if err := repo.UpdateRef(ctx, git.ReferenceName(request.TargetRef), mergeOID, oldTargetOID); err != nil {
		//nolint:stylecheck
		return nil, helper.ErrPreconditionFailed(fmt.Errorf("Could not update %s. Please refresh and try again", string(request.TargetRef)))
	}

	return &gitalypb.UserMergeToRefResponse{
		CommitId: mergeOID.String(),
	}, nil
}
