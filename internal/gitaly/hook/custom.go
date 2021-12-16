package hook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

// customHooksExecutor executes all custom hooks for a given repository and hook name
type customHooksExecutor func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) error

// CustomHookError is returned in case custom hooks return an error.
type CustomHookError error

// newCustomHooksExecutor creates a new hooks executor for custom hooks. Hooks
// are looked up and executed in the following order:
//
// 1. <repository>.git/custom_hooks/<hook_name> - per project hook
// 2. <repository>.git/custom_hooks/<hook_name>.d/* - per project hooks
// 3. <custom_hooks_dir>/hooks/<hook_name>.d/* - global hooks
//
// Any files which are either not executable or have a trailing `~` are ignored.
func (m *GitLabHookManager) newCustomHooksExecutor(repo *gitalypb.Repository, hookName string) (customHooksExecutor, error) {
	repoPath, err := m.locator.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	var hookFiles []string
	projectCustomHookFile := filepath.Join(repoPath, "custom_hooks", hookName)
	if isValidHook(projectCustomHookFile) {
		hookFiles = append(hookFiles, projectCustomHookFile)
	}

	projectCustomHookDir := filepath.Join(repoPath, "custom_hooks", fmt.Sprintf("%s.d", hookName))
	files, err := findHooks(projectCustomHookDir)
	if err != nil {
		return nil, err
	}
	hookFiles = append(hookFiles, files...)

	globalCustomHooksDir := filepath.Join(m.cfg.Hooks.CustomHooksDir, fmt.Sprintf("%s.d", hookName))
	files, err = findHooks(globalCustomHooksDir)
	if err != nil {
		return nil, err
	}
	hookFiles = append(hookFiles, files...)

	return func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
		var stdinBytes []byte
		if stdin != nil {
			stdinBytes, err = io.ReadAll(stdin)
			if err != nil {
				return err
			}
		}

		for _, hookFile := range hookFiles {
			cmd := exec.Command(hookFile, args...)
			cmd.Dir = repoPath
			c, err := command.New(ctx, cmd, bytes.NewReader(stdinBytes), stdout, stderr, env...)
			if err != nil {
				return err
			}

			if err = c.Wait(); err != nil {
				// Custom hook errors need to be handled specially when we update
				// refs via updateref.UpdaterWithHooks: their stdout and stderr must
				// not be modified, but instead used as-is as the hooks' error
				// message given that they may contain output that should be shown
				// to the user.
				return CustomHookError(fmt.Errorf("error executing %q: %w", hookFile, err))
			}
		}

		return nil
	}, nil
}

// findHooks finds valid hooks in the given directory. A hook is considered
// valid if `isValidHook()` would return `true`. Matching hooks are sorted by
// filename.
func findHooks(dir string) ([]string, error) {
	fis, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	var hookFiles []string
	for _, fi := range fis {
		hookPath := filepath.Join(dir, fi.Name())
		if isValidHook(hookPath) {
			hookFiles = append(hookFiles, hookPath)
		}
	}

	return hookFiles, nil
}

// isValidHook checks whether a given path refers to a valid hook. A path is
// not a valid hook path if any of the following conditions is true:
//
// - The path ends with a tilde.
// - The path is or points at a directory.
// - The path is not executable by the current user.
func isValidHook(path string) bool {
	if strings.HasSuffix(path, "~") {
		return false
	}

	fi, err := os.Stat(path)
	if err != nil {
		return false
	}

	if fi.IsDir() {
		return false
	}

	if unix.Access(path, unix.X_OK) != nil {
		return false
	}

	return true
}

func (m *GitLabHookManager) customHooksEnv(payload git.HooksPayload, pushOptions []string, envs []string) ([]string, error) {
	repoPath, err := m.locator.GetPath(payload.Repo)
	if err != nil {
		return nil, err
	}

	customEnvs := append(command.AllowedEnvironment(envs), pushOptionsEnv(pushOptions)...)

	objectDirectory := getEnvVar("GIT_OBJECT_DIRECTORY", envs)
	if objectDirectory == "" && payload.Repo.GetGitObjectDirectory() != "" {
		objectDirectory = filepath.Join(repoPath, payload.Repo.GetGitObjectDirectory())
	}
	if objectDirectory != "" {
		customEnvs = append(customEnvs, "GIT_OBJECT_DIRECTORY="+objectDirectory)
	}

	alternateObjectDirectories := getEnvVar("GIT_ALTERNATE_OBJECT_DIRECTORIES", envs)
	if alternateObjectDirectories == "" && len(payload.Repo.GetGitAlternateObjectDirectories()) != 0 {
		var absolutePaths []string
		for _, alternateObjectDirectory := range payload.Repo.GetGitAlternateObjectDirectories() {
			absolutePaths = append(absolutePaths, filepath.Join(repoPath, alternateObjectDirectory))
		}
		alternateObjectDirectories = strings.Join(absolutePaths, ":")
	}
	if alternateObjectDirectories != "" {
		customEnvs = append(customEnvs, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+alternateObjectDirectories)
	}

	if gitDir := filepath.Dir(m.cfg.Git.BinPath); gitDir != "." {
		// By default, we should take PATH from the given set of environment variables, if
		// it's contained in there. Otherwise, we need to take the current process's PATH
		// environment, which would also be the default injected by the command package.
		currentPath := getEnvVar("PATH", envs)
		if currentPath == "" {
			currentPath = os.Getenv("PATH")
		}

		// We want to ensure that custom hooks use the same Git version as used by Gitaly.
		// Given that our Git version may not be contained in PATH, we thus have to prepend
		// the directory containing that executable to PATH such that it can be found.
		customEnvs = append(customEnvs, fmt.Sprintf("PATH=%s:%s", gitDir, currentPath))
	}

	// We need to inject environment variables which set up the Git execution environment in
	// case we're running with bundled Git such that Git can locate its binaries.
	customEnvs = append(customEnvs, m.cfg.GitExecEnv()...)

	return append(customEnvs,
		"GIT_DIR="+repoPath,
		"GL_REPOSITORY="+payload.Repo.GetGlRepository(),
		"GL_PROJECT_PATH="+payload.Repo.GetGlProjectPath(),
		"GL_ID="+payload.ReceiveHooksPayload.UserID,
		"GL_USERNAME="+payload.ReceiveHooksPayload.Username,
		"GL_PROTOCOL="+payload.ReceiveHooksPayload.Protocol,
	), nil
}

// pushOptionsEnv turns a slice of git push option values into a GIT_PUSH_OPTION_COUNT and individual
// GIT_PUSH_OPTION_0, GIT_PUSH_OPTION_1 etc.
func pushOptionsEnv(options []string) []string {
	if len(options) == 0 {
		return []string{}
	}

	envVars := []string{fmt.Sprintf("GIT_PUSH_OPTION_COUNT=%d", len(options))}

	for i, pushOption := range options {
		envVars = append(envVars, fmt.Sprintf("GIT_PUSH_OPTION_%d=%s", i, pushOption))
	}

	return envVars
}
