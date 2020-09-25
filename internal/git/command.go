package git

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// unsafeCmdWithEnv creates a git.unsafeCmd with the given args, environment, and Repository
func unsafeCmdWithEnv(ctx context.Context, extraEnv []string, repo repository.GitRepo, args ...string) (*command.Command, error) {
	args, env, err := argsAndEnv(repo, args...)
	if err != nil {
		return nil, err
	}

	env = append(env, extraEnv...)

	return unsafeBareCmd(ctx, repo, CmdStream{}, env, args...)
}

// unsafeStdinCmd creates a git.Command with the given args and Repository that is
// suitable for Write()ing to
func unsafeStdinCmd(ctx context.Context, repo repository.GitRepo, args ...string) (*command.Command, error) {
	args, env, err := argsAndEnv(repo, args...)
	if err != nil {
		return nil, err
	}

	return unsafeBareCmd(ctx, repo, CmdStream{In: command.SetupStdin}, env, args...)
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
func unsafeBareCmd(ctx context.Context, repo repository.GitRepo, stream CmdStream, env []string, args ...string) (*command.Command, error) {
	if repo == nil {
		env = append(env, command.GitEnv...)
		return command.New(ctx, exec.Command(command.GitPath(), args...), stream.In, stream.Out, stream.Err, env...)
	}

	args = append([]string{command.GitPath()}, args...)

	if os.Getenv("GITALY_CGROUP_ENABLED") == "true" {
		// 16^3 = 4k
		repoPath, err := helper.GetRepoPath(repo)
		if err != nil {
			return nil, err
		}
		hash := sha1.Sum([]byte(repoPath))

		dst := make([]byte, hex.EncodedLen(len(hash)))
		hex.Encode(dst, hash[:])
		groupName := string(dst[0:3])

		cgroupSet, err = getCgroupSet()
		if err != nil {
			return nil, err
		}

		cgexecArgs := []string{"sudo", "cgexec"}
		for _, type_ := range []string{"cpu", "memory"} {
			cgroupName := cgroupSet[type_] + "/" + groupName
			cgexecArgs = append(cgexecArgs, "-g", type_+":"+cgroupName)
		}

		args = append(cgexecArgs, args...)
	}

	env = append(env, command.GitEnv...)
	return command.New(ctx, exec.Command(args[0], args[1:]...), stream.In, stream.Out, stream.Err, env...)
}

// unsafeCmdWithoutRepo works like Command but without a git repository
func unsafeCmdWithoutRepo(ctx context.Context, stream CmdStream, args ...string) (*command.Command, error) {
	return unsafeBareCmd(ctx, nil, stream, nil, args...)
}
