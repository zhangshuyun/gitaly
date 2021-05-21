package gitlab

import (
	"context"
)

// AllowedParams compose set of parameters required to call 'GitlabAPI.Allowed' method.
type AllowedParams struct {
	// RepoPath is an absolute path to the repository.
	RepoPath string
	// GitObjectDirectory is a path to git object directory.
	GitObjectDirectory string
	// GitAlternateObjectDirectories are the paths to alternate object directories.
	GitAlternateObjectDirectories []string
	// GLRepository is a name of the repository.
	GLRepository string
	// GLID is an identifier of the repository.
	GLID string
	// GLProtocol is a protocol used for operation.
	GLProtocol string
	// Changes is a set of changes to be applied.
	Changes string
}

// PostReceiveMessage encapsulates a message from the /post_receive endpoint that gets printed to stdout
type PostReceiveMessage struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// CheckInfo represents the response of GitLabs `check` API endpoint
type CheckInfo struct {
	// Version of the GitLab Rails component
	Version string `json:"gitlab_version"`
	// Revision of the Git object of the running GitLab
	Revision string `json:"gitlab_revision"`
	// APIVersion of GitLab, expected to be v4
	APIVersion string `json:"api_version"`
	// RedisReachable shows if GitLab can reach Redis. This can be false
	// while the check itself succeeds. Normal hook API calls will likely
	// fail.
	RedisReachable bool `json:"redis"`
}

// Client is an interface for accessing the GitLab internal API
type Client interface {
	// Allowed queries the gitlab internal api /allowed endpoint to determine if a ref change for a given repository and user is allowed
	Allowed(ctx context.Context, params AllowedParams) (bool, string, error)
	// Check verifies that GitLab can be reached, and authenticated to
	Check(ctx context.Context) (*CheckInfo, error)
	// PreReceive queries the gitlab internal api /pre_receive to increase the reference counter
	PreReceive(ctx context.Context, glRepository string) (bool, error)
	// PostReceive queries the gitlab internal api /post_receive to decrease the reference counter
	PostReceive(ctx context.Context, glRepository, glID, changes string, pushOptions ...string) (bool, []PostReceiveMessage, error)
}
