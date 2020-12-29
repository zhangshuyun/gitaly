package helper

import (
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// RepoPathEqual compares if two repositories are in the same location
func RepoPathEqual(a, b repository.GitRepo) bool {
	return a.GetStorageName() == b.GetStorageName() &&
		a.GetRelativePath() == b.GetRelativePath()
}

// ProtoRepoFromRepo allows object pools and repository abstractions to be used
// in places that require a concrete type
func ProtoRepoFromRepo(repo repository.GitRepo) *gitalypb.Repository {
	return &gitalypb.Repository{
		StorageName:                   repo.GetStorageName(),
		GitAlternateObjectDirectories: repo.GetGitAlternateObjectDirectories(),
		GitObjectDirectory:            repo.GetGitObjectDirectory(),
		RelativePath:                  repo.GetRelativePath(),
	}
}
