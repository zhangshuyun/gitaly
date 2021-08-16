package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	gitlabWorktreesSubDir = "gitlab-worktree"
	ambiguousArgumentFmt  = "fatal: ambiguous argument '%s...%s': unknown revision or path not in the working tree.\nUse '--' to separate paths from revisions, like this:\n'git <command> [<revision>...] -- [<file>...]'\n"
)

func (s *Server) UserSquash(ctx context.Context, req *gitalypb.UserSquashRequest) (*gitalypb.UserSquashResponse, error) {
	if err := validateUserSquashRequest(req); err != nil {
		return nil, helper.ErrInvalidArgumentf("UserSquash: %v", err)
	}

	if strings.Contains(req.GetSquashId(), "/") {
		return nil, helper.ErrInvalidArgument(errors.New("worktree id can't contain slashes"))
	}

	sha, err := s.userSquash(ctx, req)
	if err != nil {
		var gitErr gitError
		if errors.As(err, &gitErr) {
			if gitErr.ErrMsg != "" {
				// we log an actual error as it would be lost otherwise (it is not sent back to the client)
				ctxlogrus.Extract(ctx).WithError(err).Error("user squash")
				return &gitalypb.UserSquashResponse{GitError: gitErr.ErrMsg}, nil
			}
		}

		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.UserSquashResponse{SquashSha: sha}, nil
}

func validateUserSquashRequest(req *gitalypb.UserSquashRequest) error {
	if req.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if req.GetUser() == nil {
		return errors.New("empty User")
	}

	if len(req.GetUser().GetName()) == 0 {
		return errors.New("empty user name")
	}

	if len(req.GetUser().GetEmail()) == 0 {
		return errors.New("empty user email")
	}

	if req.GetSquashId() == "" {
		return errors.New("empty SquashId")
	}

	if req.GetStartSha() == "" {
		return errors.New("empty StartSha")
	}

	if req.GetEndSha() == "" {
		return errors.New("empty EndSha")
	}

	if len(req.GetCommitMessage()) == 0 {
		return errors.New("empty CommitMessage")
	}

	if req.GetAuthor() == nil {
		return errors.New("empty Author")
	}

	if len(req.GetAuthor().GetName()) == 0 {
		return errors.New("empty author name")
	}

	if len(req.GetAuthor().GetEmail()) == 0 {
		return errors.New("empty auithor email")
	}

	return nil
}

type gitError struct {
	// ErrMsg error message from 'git' executable if any.
	ErrMsg string
	// Err is an error that happened during rebase process.
	Err error
}

func (er gitError) Error() string {
	return er.ErrMsg + ": " + er.Err.Error()
}

func (s *Server) userSquash(ctx context.Context, req *gitalypb.UserSquashRequest) (string, error) {
	repo := s.localrepo(req.GetRepository())

	repoPath, err := repo.Path()
	if err != nil {
		return "", helper.ErrInternalf("cannot resolve repo path: %w", err)
	}

	// We need to retrieve the start commit such that we can create the new commit with
	// all parents of the start commit.
	startCommit, err := repo.ResolveRevision(ctx, git.Revision(req.GetStartSha()+"^{commit}"))
	if err != nil {
		return "", fmt.Errorf("cannot resolve start commit: %w", gitError{
			// This error is simply for backwards compatibility. We should just
			// return the plain error eventually.
			Err:    err,
			ErrMsg: fmt.Sprintf(ambiguousArgumentFmt, req.GetStartSha(), req.GetEndSha()),
		})
	}

	// And we need to take the tree of the end commit. This tree already is the result
	endCommit, err := repo.ResolveRevision(ctx, git.Revision(req.GetEndSha()+"^{commit}"))
	if err != nil {
		return "", fmt.Errorf("cannot resolve end commit's tree: %w", gitError{
			// This error is simply for backwards compatibility. We should just
			// return the plain error eventually.
			Err:    err,
			ErrMsg: fmt.Sprintf(ambiguousArgumentFmt, req.GetStartSha(), req.GetEndSha()),
		})
	}

	commitDate, err := dateFromProto(req)
	if err != nil {
		return "", helper.ErrInvalidArgument(err)
	}

	// We're now rebasing the end commit on top of the start commit. The resulting tree
	// is then going to be the tree of the squashed commit.
	rebasedCommitID, err := s.git2go.Rebase(ctx, repo, git2go.RebaseCommand{
		Repository: repoPath,
		Committer: git2go.NewSignature(
			string(req.GetUser().Name), string(req.GetUser().Email), commitDate,
		),
		CommitID:         endCommit,
		UpstreamCommitID: startCommit,
	})
	if err != nil {
		return "", fmt.Errorf("rebasing end onto start commit: %w", gitError{
			Err:    err,
			ErrMsg: err.Error(),
		})
	}

	treeID, err := repo.ResolveRevision(ctx, rebasedCommitID.Revision()+"^{tree}")
	if err != nil {
		return "", fmt.Errorf("cannot resolve rebased tree: %w", err)
	}

	commitEnv := []string{
		"GIT_COMMITTER_NAME=" + string(req.GetUser().Name),
		"GIT_COMMITTER_EMAIL=" + string(req.GetUser().Email),
		fmt.Sprintf("GIT_COMMITTER_DATE=%d %s", commitDate.Unix(), commitDate.Format("-0700")),
		"GIT_AUTHOR_NAME=" + string(req.GetAuthor().Name),
		"GIT_AUTHOR_EMAIL=" + string(req.GetAuthor().Email),
		fmt.Sprintf("GIT_AUTHOR_DATE=%d %s", commitDate.Unix(), commitDate.Format("-0700")),
	}

	flags := []git.Option{
		git.ValueFlag{
			Name:  "-m",
			Value: string(req.GetCommitMessage()),
		},
		git.ValueFlag{
			Name:  "-p",
			Value: startCommit.String(),
		},
	}

	var stdout, stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name:  "commit-tree",
		Flags: flags,
		Args: []string{
			treeID.String(),
		},
	}, git.WithStdout(&stdout), git.WithStderr(&stderr), git.WithEnv(commitEnv...)); err != nil {
		return "", fmt.Errorf("creating commit: %w", gitError{
			Err:    err,
			ErrMsg: stderr.String(),
		})
	}

	return text.ChompBytes(stdout.Bytes()), nil
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
