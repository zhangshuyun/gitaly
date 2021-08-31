package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNoDefaultBranch = errors.New("no default branch")

func (s *Server) UserApplyPatch(stream gitalypb.OperationService_UserApplyPatchServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "UserApplyPatch: empty UserApplyPatch_Header")
	}

	if err := validateUserApplyPatchHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "UserApplyPatch: %v", err)
	}

	if err := s.userApplyPatch(stream.Context(), header, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *Server) userApplyPatch(ctx context.Context, header *gitalypb.UserApplyPatchRequest_Header, stream gitalypb.OperationService_UserApplyPatchServer) error {
	path, err := s.locator.GetRepoPath(header.Repository)
	if err != nil {
		return err
	}

	branchCreated := false
	targetBranch := git.NewReferenceNameFromBranchName(string(header.TargetBranch))

	repo := s.localrepo(header.Repository)
	parentCommitID, err := repo.ResolveRevision(ctx, targetBranch.Revision()+"^{commit}")
	if err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) {
			return fmt.Errorf("resolve target branch: %w", err)
		}

		defaultBranch, err := repo.GetDefaultBranch(ctx)
		if err != nil {
			return fmt.Errorf("default branch name: %w", err)
		} else if len(defaultBranch) == 0 {
			return errNoDefaultBranch
		}

		branchCreated = true
		parentCommitID, err = repo.ResolveRevision(ctx, defaultBranch.Revision()+"^{commit}")
		if err != nil {
			return fmt.Errorf("resolve default branch commit: %w", err)
		}
	}

	// This should be changed to consider timezones after the Ruby implementation is removed.
	// https://gitlab.com/gitlab-org/gitaly/-/issues/3711
	committerTime := time.Now()
	if header.Timestamp != nil {
		committerTime = header.Timestamp.AsTime()
	}

	worktreePath := newWorktreePath(path, "am-")
	if err := s.addWorktree(ctx, header.Repository, worktreePath, parentCommitID.String()); err != nil {
		return fmt.Errorf("add worktree: %w", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(helper.SuppressCancellation(ctx), 30*time.Second)
		defer cancel()

		worktreeName := filepath.Base(worktreePath)
		if err := s.removeWorktree(ctx, header.Repository, worktreeName); err != nil {
			ctxlogrus.Extract(ctx).WithField("worktree_name", worktreeName).WithError(err).Error("failed to remove worktree")
		}
	}()

	var stdout, stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name: "am",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--3way"},
			},
		},
		git.WithEnv(
			"GIT_COMMITTER_NAME="+string(header.GetUser().Name),
			"GIT_COMMITTER_EMAIL="+string(header.GetUser().Email),
			fmt.Sprintf("GIT_COMMITTER_DATE=%d %s", committerTime.Unix(), committerTime.Format("-0700")),
		),
		git.WithStdin(streamio.NewReader(func() ([]byte, error) {
			req, err := stream.Recv()
			return req.GetPatches(), err
		})),
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, header.Repository, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("create git am: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		// The Ruby implementation doesn't include stderr in errors, which makes
		// it difficult to determine the cause of an error. This special cases the
		// user facing patching error which is returned usually to maintain test
		// compatibility but returns the error and stderr otherwise. Once the Ruby
		// implementation is removed, this should probably be dropped.
		if bytes.HasPrefix(stdout.Bytes(), []byte("Patch failed at")) {
			return status.Error(codes.FailedPrecondition, stdout.String())
		}

		return fmt.Errorf("apply patch: %w, stderr: %q", err, &stderr)
	}

	var revParseStdout, revParseStderr bytes.Buffer
	revParseCmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name: "rev-parse",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--verify"},
			},
			Args: []string{"HEAD^{commit}"},
		},
		git.WithStdout(&revParseStdout),
		git.WithStderr(&revParseStderr),
	)
	if err != nil {
		return fmt.Errorf("create git rev-parse: %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	if err := revParseCmd.Wait(); err != nil {
		return fmt.Errorf("get patched commit: %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	patchedCommit, err := git.NewObjectIDFromHex(text.ChompBytes(revParseStdout.Bytes()))
	if err != nil {
		return fmt.Errorf("parse patched commit oid: %w", err)
	}

	currentCommit := parentCommitID
	if branchCreated {
		currentCommit = git.ZeroOID
	}

	if err := s.updateReferenceWithHooks(ctx, header.Repository, header.User, nil, targetBranch, patchedCommit, currentCommit); err != nil {
		return fmt.Errorf("update reference: %w", err)
	}

	if err := stream.SendAndClose(&gitalypb.UserApplyPatchResponse{
		BranchUpdate: &gitalypb.OperationBranchUpdate{
			CommitId:      patchedCommit.String(),
			BranchCreated: branchCreated,
		},
	}); err != nil {
		return fmt.Errorf("send and close: %w", err)
	}

	return nil
}

func validateUserApplyPatchHeader(header *gitalypb.UserApplyPatchRequest_Header) error {
	if header.GetRepository() == nil {
		return fmt.Errorf("missing Repository")
	}

	if header.GetUser() == nil {
		return fmt.Errorf("missing User")
	}

	if len(header.GetTargetBranch()) == 0 {
		return fmt.Errorf("missing Branch")
	}

	return nil
}

func (s *Server) addWorktree(ctx context.Context, repo *gitalypb.Repository, worktreePath string, committish string) error {
	if err := s.runCmd(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.splitIndex", Value: "false"}}, nil); err != nil {
		return fmt.Errorf("on 'git config core.splitIndex false': %w", err)
	}

	args := []string{worktreePath}
	flags := []git.Option{git.Flag{Name: "--detach"}}
	if committish != "" {
		args = append(args, committish)
	} else {
		flags = append(flags, git.Flag{Name: "--no-checkout"})
	}

	var stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "worktree",
			Action: "add",
			Flags:  flags,
			Args:   args,
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("creation of 'git worktree add': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("wait for 'git worktree add': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) removeWorktree(ctx context.Context, repo *gitalypb.Repository, worktreeName string) error {
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

func newWorktreePath(repoPath, prefix string) string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(chars), func(i, j int) { chars[i], chars[j] = chars[j], chars[i] })
	return filepath.Join(repoPath, gitlabWorktreesSubDir, prefix+string(chars[:32]))
}

func (s *Server) runCmd(ctx context.Context, repo *gitalypb.Repository, cmd string, opts []git.Option, args []string) error {
	var stderr bytes.Buffer
	safeCmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{Name: cmd, Flags: opts, Args: args}, git.WithStderr(&stderr))
	if err != nil {
		return fmt.Errorf("create safe cmd %q: %w", cmd, gitError{ErrMsg: stderr.String(), Err: err})
	}

	if err := safeCmd.Wait(); err != nil {
		return fmt.Errorf("wait safe cmd %q: %w", cmd, gitError{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}
