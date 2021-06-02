package fieldextractors

import (
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type repositoryBasedRequest interface {
	GetRepository() *gitalypb.Repository
}

type namespaceBasedRequest interface {
	storageBasedRequest
	GetName() string
}

type storageBasedRequest interface {
	GetStorageName() string
}

func formatRepoRequest(repo *gitalypb.Repository) map[string]interface{} {
	if repo == nil {
		// Signals that the client did not send a repo through, which
		// will be useful for logging
		return map[string]interface{}{
			"repo": nil,
		}
	}

	return map[string]interface{}{
		"repoStorage":   repo.StorageName,
		"repoPath":      repo.RelativePath,
		"glRepository":  repo.GlRepository,
		"glProjectPath": repo.GlProjectPath,
	}
}

func formatStorageRequest(storageReq storageBasedRequest) map[string]interface{} {
	return map[string]interface{}{
		"StorageName": storageReq.GetStorageName(),
	}
}

func formatNamespaceRequest(namespaceReq namespaceBasedRequest) map[string]interface{} {
	return map[string]interface{}{
		"StorageName": namespaceReq.GetStorageName(),
		"Name":        namespaceReq.GetName(),
	}
}

func formatRenameNamespaceRequest(renameReq *gitalypb.RenameNamespaceRequest) map[string]interface{} {
	return map[string]interface{}{
		"StorageName": renameReq.GetStorageName(),
		"From":        renameReq.GetFrom(),
		"To":          renameReq.GetTo(),
	}
}

// FieldExtractor will extract the relevant fields from an incoming grpc request
func FieldExtractor(fullMethod string, req interface{}) map[string]interface{} {
	if req == nil {
		return nil
	}

	var result map[string]interface{}

	switch req := req.(type) {
	case *gitalypb.RenameNamespaceRequest:
		result = formatRenameNamespaceRequest(req)
	case repositoryBasedRequest:
		result = formatRepoRequest(req.GetRepository())
	case namespaceBasedRequest:
		result = formatNamespaceRequest(req)
	case storageBasedRequest:
		result = formatStorageRequest(req)
	}

	if result == nil {
		result = make(map[string]interface{})
	}

	switch {
	case strings.HasPrefix(fullMethod, "/gitaly.ObjectPoolService/"):
		addObjectPool(req, result)
	}

	result["fullMethod"] = fullMethod

	return result
}

type objectPoolRequest interface {
	GetObjectPool() *gitalypb.ObjectPool
}

func addObjectPool(req interface{}, tags map[string]interface{}) {
	oReq, ok := req.(objectPoolRequest)
	if !ok {
		return
	}

	pool := oReq.GetObjectPool()
	if pool == nil {
		return
	}

	repo := pool.GetRepository()
	if repo == nil {
		return
	}

	for k, v := range map[string]string{
		"pool.storage":           repo.StorageName,
		"pool.relativePath":      repo.RelativePath,
		"pool.sourceProjectPath": repo.GlProjectPath,
	} {
		tags[k] = v
	}
}
