package git

import (
	"context"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/repository"
)

// unsafeCmdWithEnv creates a git.unsafeCmd with the given args, environment, and Repository
func unsafeCmdWithEnv(ctx context.Context, extraEnv []string, repo repository.GitRepo, args ...string) (*command.Command, error) {
	args, env, err := argsAndEnv(repo, args...)
	if err != nil {
		return nil, err
	}

	env = append(env, extraEnv...)

	return unsafeBareCmd(ctx, CmdStream{}, env, args...)
}

// unsafeStdinCmd creates a git.Command with the given args and Repository that is
// suitable for Write()ing to
func unsafeStdinCmd(ctx context.Context, repo repository.GitRepo, args ...string) (*command.Command, error) {
	args, env, err := argsAndEnv(repo, args...)
	if err != nil {
		return nil, err
	}

	return unsafeBareCmd(ctx, CmdStream{In: command.SetupStdin}, env, args...)
}

func argsAndEnv(repo repository.GitRepo, args ...string) ([]string, []string, error) {
	repoPath, env, err := alternates.PathAndEnv(repo)
	if err != nil {
		return nil, nil, err
	}

	args = append([]string{"--git-dir", repoPath}, args...)

	return args, env, nil
}

// unsafeBareCmd creates a git.Command with the given args, stdin/stdout/stderr, and env
func unsafeBareCmd(ctx context.Context, stream CmdStream, env []string, args ...string) (*command.Command, error) {
	env = append(env, command.GitEnv...)

	return command.New(ctx, exec.Command(command.GitPath(), args...), stream.In, stream.Out, stream.Err, env...)
}

// unsafeCmdWithoutRepo works like Command but without a git repository
func unsafeCmdWithoutRepo(ctx context.Context, stream CmdStream, args ...string) (*command.Command, error) {
	return unsafeBareCmd(ctx, stream, nil, args...)
}
