package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

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

// Check performs an HTTP request to the internal/check API endpoint to verify
// the connection and tokens. It returns basic information of the installed
// GitLab
func (a *gitlabAPI) Check(ctx context.Context) (*CheckInfo, error) {
	resp, err := a.client.Get(ctx, "/check")
	if err != nil {
		return nil, fmt.Errorf("HTTP GET to GitLab endpoint /check failed: %w", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Check HTTP request failed with status: %d", resp.StatusCode)
	}

	var info CheckInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response from /check endpoint: %w", err)
	}

	return &info, nil
}
