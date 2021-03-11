package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	squashWorktreePrefix  = "squash"
	gitlabWorktreesSubDir = "gitlab-worktree"
)

func (s *Server) UserSquash(ctx context.Context, req *gitalypb.UserSquashRequest) (*gitalypb.UserSquashResponse, error) {
	if err := validateUserSquashRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "UserSquash: %v", err)
	}

	if strings.Contains(req.GetSquashId(), "/") {
		return nil, helper.ErrInvalidArgument(errors.New("worktree id can't contain slashes"))
	}

	repo := req.GetRepository()
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return nil, helper.ErrInternal(fmt.Errorf("repo path: %w", err))
	}
	env := alternates.Env(repoPath, repo.GetGitObjectDirectory(), repo.GetGitAlternateObjectDirectories())

	sha, err := s.userSquash(ctx, req, env, repoPath)
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
		return fmt.Errorf("empty Repository")
	}

	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetSquashId() == "" {
		return fmt.Errorf("empty SquashId")
	}

	if req.GetStartSha() == "" {
		return fmt.Errorf("empty StartSha")
	}

	if req.GetEndSha() == "" {
		return fmt.Errorf("empty EndSha")
	}

	if len(req.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}

	if req.GetAuthor() == nil {
		return fmt.Errorf("empty Author")
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

func (s *Server) userSquash(ctx context.Context, req *gitalypb.UserSquashRequest, env []string, repoPath string) (string, error) {
	sparseDiffFiles, err := s.diffFiles(ctx, env, repoPath, req)
	if err != nil {
		return "", fmt.Errorf("define diff files: %w", err)
	}

	if len(sparseDiffFiles) == 0 {
		sha, err := s.userSquashWithNoDiff(ctx, req, repoPath, env)
		if err != nil {
			return "", fmt.Errorf("without sparse diff: %w", err)
		}

		return sha, nil
	}

	sha, err := s.userSquashWithDiffInFiles(ctx, req, repoPath, env, sparseDiffFiles)
	if err != nil {
		return "", fmt.Errorf("with sparse diff: %w", err)
	}

	return sha, nil
}

func (s *Server) diffFiles(ctx context.Context, env []string, repoPath string, req *gitalypb.UserSquashRequest) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.NewWithDir(ctx, repoPath,
		git.SubCmd{
			Name:  "diff",
			Flags: []git.Option{git.Flag{Name: "--name-only"}, git.Flag{Name: "--diff-filter=ar"}, git.Flag{Name: "--binary"}},
			Args:  []string{diffRange(req)},
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

var errNoFilesCheckedOut = errors.New("no files checked out")

func (s *Server) userSquashWithDiffInFiles(ctx context.Context, req *gitalypb.UserSquashRequest, repoPath string, env []string, diffFilesOut []byte) (string, error) {
	repo := req.GetRepository()
	worktreePath := newSquashWorktreePath(repoPath, req.GetSquashId())

	if err := s.addWorktree(ctx, repo, worktreePath, ""); err != nil {
		return "", fmt.Errorf("add worktree: %w", err)
	}

	defer func(worktreeName string) {
		ctx, cancel := context.WithCancel(command.SuppressCancellation(ctx))
		defer cancel()

		if err := s.removeWorktree(ctx, repo, worktreeName); err != nil {
			ctxlogrus.Extract(ctx).WithField("worktree_name", worktreeName).WithError(err).Error("failed to remove worktree")
		}
	}(filepath.Base(worktreePath))

	worktreeGitPath, err := s.revParseGitDir(ctx, worktreePath)
	if err != nil {
		return "", fmt.Errorf("define git dir for worktree: %w", err)
	}

	if err := s.runCmd(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.sparseCheckout", Value: "true"}}, nil); err != nil {
		return "", fmt.Errorf("on 'git config core.sparseCheckout true': %w", err)
	}

	if err := s.createSparseCheckoutFile(worktreeGitPath, diffFilesOut); err != nil {
		return "", fmt.Errorf("create sparse checkout file: %w", err)
	}

	if err := s.checkout(ctx, repo, worktreePath, req); err != nil {
		if !errors.Is(err, errNoFilesCheckedOut) {
			return "", fmt.Errorf("perform 'git checkout' with core.sparseCheckout true: %w", err)
		}

		// try to perform checkout with disabled sparseCheckout feature
		if err := s.runCmd(ctx, repo, "config", []git.Option{git.ConfigPair{Key: "core.sparseCheckout", Value: "false"}}, nil); err != nil {
			return "", fmt.Errorf("on 'git config core.sparseCheckout false': %w", err)
		}

		if err := s.checkout(ctx, repo, worktreePath, req); err != nil {
			return "", fmt.Errorf("perform 'git checkout' with core.sparseCheckout false: %w", err)
		}
	}

	sha, err := s.applyDiff(ctx, repo, req, worktreePath, env)
	if err != nil {
		return "", fmt.Errorf("apply diff: %w", err)
	}

	return sha, nil
}

func (s *Server) checkout(ctx context.Context, repo *gitalypb.Repository, worktreePath string, req *gitalypb.UserSquashRequest) error {
	var stderr bytes.Buffer
	checkoutCmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name:  "checkout",
			Flags: []git.Option{git.Flag{Name: "--detach"}},
			Args:  []string{req.GetStartSha()},
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		return fmt.Errorf("create 'git checkout': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	if err = checkoutCmd.Wait(); err != nil {
		if strings.Contains(stderr.String(), "error: Sparse checkout leaves no entry on working directory") {
			return errNoFilesCheckedOut
		}

		return fmt.Errorf("wait for 'git checkout': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return nil
}

func (s *Server) revParseGitDir(ctx context.Context, worktreePath string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name:  "rev-parse",
			Flags: []git.Option{git.Flag{Name: "--git-dir"}},
		},
		git.WithStdout(&stdout),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return "", fmt.Errorf("creation of 'git rev-parse --git-dir': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	if err := cmd.Wait(); err != nil {
		return "", fmt.Errorf("wait for 'git rev-parse --git-dir': %w", gitError{ErrMsg: stderr.String(), Err: err})
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

func (s *Server) userSquashWithNoDiff(ctx context.Context, req *gitalypb.UserSquashRequest, repoPath string, env []string) (string, error) {
	repo := req.GetRepository()
	worktreePath := newSquashWorktreePath(repoPath, req.GetSquashId())

	if err := s.addWorktree(ctx, repo, worktreePath, req.GetStartSha()); err != nil {
		return "", fmt.Errorf("add worktree: %w", err)
	}

	defer func(worktreeName string) {
		ctx, cancel := context.WithCancel(command.SuppressCancellation(ctx))
		defer cancel()

		if err := s.removeWorktree(ctx, repo, worktreeName); err != nil {
			ctxlogrus.Extract(ctx).WithField("worktree_name", worktreeName).WithError(err).Error("failed to remove worktree")
		}
	}(filepath.Base(worktreePath))

	sha, err := s.applyDiff(ctx, repo, req, worktreePath, env)
	if err != nil {
		return "", fmt.Errorf("apply diff: %w", err)
	}

	return sha, nil
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

func (s *Server) applyDiff(ctx context.Context, repo *gitalypb.Repository, req *gitalypb.UserSquashRequest, worktreePath string, env []string) (string, error) {
	diffRange := diffRange(req)

	var diffStderr bytes.Buffer
	cmdDiff, err := s.gitCmdFactory.New(ctx, req.GetRepository(),
		git.SubCmd{
			Name: "diff",
			Flags: []git.Option{
				git.Flag{Name: "--binary"},
			},
			Args: []string{diffRange},
		},
		git.WithStderr(&diffStderr),
	)
	if err != nil {
		return "", fmt.Errorf("creation of 'git diff' for range %q: %w", diffRange, gitError{ErrMsg: diffStderr.String(), Err: err})
	}

	var applyStderr bytes.Buffer
	cmdApply, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath,
		git.SubCmd{
			Name: "apply",
			Flags: []git.Option{
				git.Flag{Name: "--index"},
				git.Flag{Name: "--3way"},
				git.Flag{Name: "--whitespace=nowarn"},
			},
		},
		git.WithEnv(env...),
		git.WithStdin(command.SetupStdin),
		git.WithStderr(&applyStderr),
	)
	if err != nil {
		return "", fmt.Errorf("creation of 'git apply' for range %q: %w", diffRange, gitError{ErrMsg: applyStderr.String(), Err: err})
	}

	if _, err := io.Copy(cmdApply, cmdDiff); err != nil {
		return "", fmt.Errorf("piping 'git diff' -> 'git apply' for range %q: %w", diffRange, gitError{ErrMsg: applyStderr.String(), Err: err})
	}

	if err := cmdDiff.Wait(); err != nil {
		return "", fmt.Errorf("wait for 'git diff' for range %q: %w", diffRange, gitError{ErrMsg: diffStderr.String(), Err: err})
	}

	if err := cmdApply.Wait(); err != nil {
		return "", fmt.Errorf("wait for 'git apply' for range %q: %w", diffRange, gitError{ErrMsg: applyStderr.String(), Err: err})
	}

	commitDate := time.Now()
	if req.Timestamp != nil {
		commitDate, err = ptypes.Timestamp(req.Timestamp)
		if err != nil {
			return "", helper.ErrInvalidArgument(err)
		}
	}

	commitEnv := append(env,
		"GIT_COMMITTER_NAME="+string(req.GetUser().Name),
		"GIT_COMMITTER_EMAIL="+string(req.GetUser().Email),
		fmt.Sprintf("GIT_COMMITTER_DATE=%d +0000", commitDate.Unix()),
		"GIT_AUTHOR_NAME="+string(req.GetAuthor().Name),
		"GIT_AUTHOR_EMAIL="+string(req.GetAuthor().Email),
		fmt.Sprintf("GIT_AUTHOR_DATE=%d +0000", commitDate.Unix()),
	)

	var commitStderr bytes.Buffer
	cmdCommit, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath, git.SubCmd{
		Name: "commit",
		Flags: []git.Option{
			git.Flag{Name: "--no-verify"},
			git.Flag{Name: "--quiet"},
			git.ValueFlag{Name: "--message", Value: string(req.GetCommitMessage())},
		},
	}, git.WithEnv(commitEnv...), git.WithStderr(&commitStderr), git.WithRefTxHook(ctx, repo, s.cfg))
	if err != nil {
		return "", fmt.Errorf("creation of 'git commit': %w", gitError{ErrMsg: commitStderr.String(), Err: err})
	}

	if err := cmdCommit.Wait(); err != nil {
		return "", fmt.Errorf("wait for 'git commit': %w", gitError{ErrMsg: commitStderr.String(), Err: err})
	}

	var revParseStdout, revParseStderr bytes.Buffer
	revParseCmd, err := s.gitCmdFactory.NewWithDir(ctx, worktreePath, git.SubCmd{
		Name: "rev-parse",
		Flags: []git.Option{
			git.Flag{Name: "--quiet"},
			git.Flag{Name: "--verify"},
		},
		Args: []string{"HEAD^{commit}"},
	}, git.WithEnv(env...), git.WithStdout(&revParseStdout), git.WithStderr(&revParseStderr))
	if err != nil {
		return "", fmt.Errorf("creation of 'git rev-parse': %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	if err := revParseCmd.Wait(); err != nil {
		return "", fmt.Errorf("wait for 'git rev-parse': %w", gitError{ErrMsg: revParseStderr.String(), Err: err})
	}

	return text.ChompBytes(revParseStdout.Bytes()), nil
}

func (s *Server) createSparseCheckoutFile(worktreeGitPath string, diffFilesOut []byte) error {
	if err := os.MkdirAll(filepath.Join(worktreeGitPath, "info"), 0755); err != nil {
		return fmt.Errorf("create 'info' dir for worktree %q: %w", worktreeGitPath, err)
	}

	if err := ioutil.WriteFile(filepath.Join(worktreeGitPath, "info", "sparse-checkout"), diffFilesOut, 0666); err != nil {
		return fmt.Errorf("create 'sparse-checkout' file for worktree %q: %w", worktreeGitPath, err)
	}

	return nil
}

func diffRange(req *gitalypb.UserSquashRequest) string {
	return req.GetStartSha() + "..." + req.GetEndSha()
}

func newSquashWorktreePath(repoPath, squashID string) string {
	prefix := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Shuffle(len(prefix), func(i, j int) { prefix[i], prefix[j] = prefix[j], prefix[i] })

	worktreeName := squashWorktreePrefix + "-" + squashID + "-" + string(prefix[:32])
	return filepath.Join(repoPath, gitlabWorktreesSubDir, worktreeName)
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
