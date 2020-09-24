package git

import (
	"context"
	"crypto/sha1"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"github.com/containerd/cgroups"
	specs "github.com/opencontainers/runtime-spec/specs-go"
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
		return command.New(ctx, exec.Command(command.GitPath(), args...), stream.In, stream.Out, stream.Err, env...)
	}

	// TODO: extract this out into a platform-specific package

	// 16^3 = 4k
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}
	hash := sha1.Sum([]byte(repoPath))
	groupName := string(hash[0:3])

	subCgroup, err := cgroups.Load(cgroups.V1, cgroups.NestedPath(groupName))
	if err != nil && err != cgroups.ErrCgroupDeleted {
		return nil, err
	}

	if err == cgroups.ErrCgroupDeleted {
		// cgroup does not yet exist, let's create it
		control, err := cgroups.Load(cgroups.V1, cgroups.NestedPath(""))
		if err != nil {
			return nil, err
		}

		// TODO: adjust limits dynamically, possibly based on percentage
		// 100/1024 and 1GiB for now
		cpuShares := uint64(100)
		memoryLimit := int64(1024 * 1024 * 1024)
		subCgroup, err = control.New(groupName, &specs.LinuxResources{
			CPU: &specs.LinuxCPU{
				Shares: &cpuShares,
			},
			Memory: &specs.LinuxMemory{
				Limit: &memoryLimit,
			},
		})
		if err != nil {
			return nil, err
		}
	}

	env = append(env, command.GitEnv...)

	cmd, err := command.New(ctx, exec.Command(command.GitPath(), args...), stream.In, stream.Out, stream.Err, env...)
	if err != nil {
		return nil, err
	}

	if err := subCgroup.Add(cgroups.Process{Pid: cmd.Pid()}); err != nil {
		return nil, err
	}

	// TODO: check if we need to add children

	return cmd, err
}

// unsafeCmdWithoutRepo works like Command but without a git repository
func unsafeCmdWithoutRepo(ctx context.Context, stream CmdStream, args ...string) (*command.Command, error) {
	return unsafeBareCmd(ctx, nil, stream, nil, args...)
}
