package hook

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/sys/unix"
)

// customHooksExecutor executes all custom hooks for a given repository and hook name
type customHooksExecutor func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) error

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
	s, err := os.Stat(projectCustomHookFile)
	if err == nil && s.Mode()&0100 != 0 {
		hookFiles = append(hookFiles, projectCustomHookFile)
	}

	projectCustomHookDir := filepath.Join(repoPath, "custom_hooks", fmt.Sprintf("%s.d", hookName))
	files, err := matchFiles(projectCustomHookDir)
	if err != nil {
		return nil, err
	}
	hookFiles = append(hookFiles, files...)

	globalCustomHooksDir := filepath.Join(m.hooksConfig.CustomHooksDir, fmt.Sprintf("%s.d", hookName))

	files, err = matchFiles(globalCustomHooksDir)
	if err != nil {
		return nil, err
	}
	hookFiles = append(hookFiles, files...)

	return func(ctx context.Context, args, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
		var stdinBytes []byte
		if stdin != nil {
			stdinBytes, err = ioutil.ReadAll(stdin)
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
				return err
			}
		}

		return nil
	}, nil
}

// match files from path:
// 1. file must be executable
// 2. file must not match backup file
//
// the resulting list is sorted
func matchFiles(dir string) ([]string, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	var hookFiles []string

	for _, fi := range fis {
		if fi.IsDir() || strings.HasSuffix(fi.Name(), "~") {
			continue
		}

		filename := filepath.Join(dir, fi.Name())

		if fi.Mode()&os.ModeSymlink != 0 {
			path, err := filepath.EvalSymlinks(filename)
			if err != nil {
				continue
			}

			fi, err = os.Lstat(path)
			if err != nil {
				continue
			}
		}

		if !validHook(fi, filename) {
			continue
		}

		hookFiles = append(hookFiles, filename)
	}

	return hookFiles, nil
}

func validHook(fi os.FileInfo, filename string) bool {
	if fi.IsDir() {
		return false
	}
	if unix.Access(filename, unix.X_OK) != nil {
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

	for _, env := range envs {
		if strings.HasPrefix(env, "GIT_OBJECT_DIRECTORY=") || strings.HasPrefix(env, "GIT_ALTERNATE_OBJECT_DIRECTORIES=") {
			customEnvs = append(customEnvs, env)
		}
	}

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
