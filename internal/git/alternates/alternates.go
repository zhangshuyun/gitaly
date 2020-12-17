package alternates

import (
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// PathAndEnv finds the disk path to a repository, and returns the
// alternate object directory environment variables encoded in the
// gitalypb.Repository instance.
// Deprecated: please use storage.Locator to define the project path and alternates.Env
// to get alternate object directory environment variables.
func PathAndEnv(repo repository.GitRepo) (string, []string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", nil, err
	}

	env := Env(repoPath, repo.GetGitObjectDirectory(), repo.GetGitAlternateObjectDirectories())

	return repoPath, env, nil
}

// Env returns the alternate object directory environment variables.
func Env(repoPath, objectDirectory string, alternateObjectDirectories []string) []string {
	var env []string
	if objectDirectory != "" {
		env = append(env, fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", filepath.Join(repoPath, objectDirectory)))
	}

	if len(alternateObjectDirectories) > 0 {
		var dirsList []string

		for _, dir := range alternateObjectDirectories {
			dirsList = append(dirsList, filepath.Join(repoPath, dir))
		}

		env = append(env, fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", strings.Join(dirsList, ":")))
	}

	return env
}
