package repotx

import "gitlab.com/gitlab-org/gitaly/internal/git/repository"

type Transaction struct {
	serviceName string
	methodName  string
	repo        repository.GitRepo
	repository.RepoLock
}
