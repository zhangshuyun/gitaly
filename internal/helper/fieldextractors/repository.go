package fieldextractors

import pb "gitlab.com/gitlab-org/gitaly-proto/go"

func formatRepoRequest(repo *pb.Repository) map[string]interface{} {
	if repo == nil {
		// Signals that the client did not send a repo through, which
		// will be useful for logging
		return map[string]interface{}{
			"repo": nil,
		}
	}

	return map[string]interface{}{
		"repoStorage": repo.StorageName,
		"repoPath":    repo.RelativePath,
	}
}

type repositoryBasedRequest interface {
	GetRepository() *pb.Repository
}

// RepositoryFieldExtractor will extract the repository fields from an incoming grpc request
func RepositoryFieldExtractor(fullMethod string, req interface{}) map[string]interface{} {
	if req == nil {
		return nil
	}

	if repoReq, ok := req.(repositoryBasedRequest); ok {
		return formatRepoRequest(repoReq.GetRepository())
	}

	// Add other request handlers here in future
	return nil

}
