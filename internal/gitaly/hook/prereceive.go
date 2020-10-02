package hook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func getRelativeObjectDirs(repoPath, gitObjectDir, gitAlternateObjectDirs string) (string, []string, error) {
	repoPathReal, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		return "", nil, err
	}

	gitObjDirRel, err := filepath.Rel(repoPathReal, gitObjectDir)
	if err != nil {
		return "", nil, err
	}

	var gitAltObjDirsRel []string

	for _, gitAltObjDirAbs := range strings.Split(gitAlternateObjectDirs, ":") {
		gitAltObjDirRel, err := filepath.Rel(repoPathReal, gitAltObjDirAbs)
		if err != nil {
			return "", nil, err
		}

		gitAltObjDirsRel = append(gitAltObjDirsRel, gitAltObjDirRel)
	}

	return gitObjDirRel, gitAltObjDirsRel, nil
}

func (m *GitLabHookManager) PreReceiveHook(ctx context.Context, repo *gitalypb.Repository, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternalf("reading stdin from request: %w", err)
	}

	primary, err := isPrimary(env)
	if err != nil {
		return helper.ErrInternalf("could not check role: %w", err)
	}

	// Only the primary should execute hooks and increment reference counters.
	if primary {
		if err := m.preReceiveHook(ctx, repo, env, changes, stdout, stderr); err != nil {
			return err
		}
	}

	return nil
}

func (m *GitLabHookManager) preReceiveHook(ctx context.Context, repo *gitalypb.Repository, env []string, changes []byte, stdout, stderr io.Writer) error {
	if gitObjDir, gitAltObjDirs := getEnvVar("GIT_OBJECT_DIRECTORY", env), getEnvVar("GIT_ALTERNATE_OBJECT_DIRECTORIES", env); gitObjDir != "" && gitAltObjDirs != "" {
		repoPath, err := helper.GetRepoPath(repo)
		if err != nil {
			return helper.ErrInternalf("getting repo path: %v", err)
		}

		gitObjectDirRel, gitAltObjectDirRel, err := getRelativeObjectDirs(repoPath, gitObjDir, gitAltObjDirs)
		if err != nil {
			return helper.ErrInternalf("getting relative git object directories: %v", err)
		}

		repo.GitObjectDirectory = gitObjectDirRel
		repo.GitAlternateObjectDirectories = gitAltObjectDirRel
	}

	glID, glRepo, glProtocol := getEnvVar("GL_ID", env), getEnvVar("GL_REPOSITORY", env), getEnvVar("GL_PROTOCOL", env)

	allowed, message, err := m.gitlabAPI.Allowed(ctx, repo, glRepo, glID, glProtocol, string(changes))
	if err != nil {
		return fmt.Errorf("GitLab: %v", err)
	}

	if !allowed {
		return errors.New(message)
	}

	executor, err := m.newCustomHooksExecutor(repo, "pre-receive")
	if err != nil {
		return fmt.Errorf("creating custom hooks executor: %w", err)
	}

	if err = executor(
		ctx,
		nil,
		env,
		bytes.NewReader(changes),
		stdout,
		stderr,
	); err != nil {
		return fmt.Errorf("executing custom hooks: %w", err)
	}

	// reference counter
	ok, err := m.gitlabAPI.PreReceive(ctx, glRepo)
	if err != nil {
		return helper.ErrInternalf("calling pre_receive endpoint: %v", err)
	}

	if !ok {
		return errors.New("")
	}

	return nil
}
