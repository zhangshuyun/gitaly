package alternates

import (
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
)

// PathAndEnv finds the disk path to a repository, and returns the
// alternate object directory environment variables encoded in the
// gitalypb.Repository instance.
func PathAndEnv(repo repository.GitRepo) (string, []string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", nil, err
	}

	var env []string
	if dir := repo.GetGitObjectDirectory(); dir != "" {
		env = append(env, fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", filepath.Join(repoPath, dir)))
	}

	if dirs := repo.GetGitAlternateObjectDirectories(); len(dirs) > 0 {
		var dirsList []string

		for _, dir := range dirs {
			dirsList = append(dirsList, filepath.Join(repoPath, dir))
		}

		env = append(env, fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", strings.Join(dirsList, ":")))
	}

	return repoPath, env, nil
}
