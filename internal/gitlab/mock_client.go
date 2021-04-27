package gitlab

import (
	"context"
)

// MockClient is a mock client of the internal GitLab API.
type MockClient struct{}

// NewMockClient returns a new mock client for the internal GitLab API.
func NewMockClient() Client {
	return &MockClient{}
}

// Allowed does nothing and always returns true.
func (m *MockClient) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	return true, "", nil
}

// Check does nothing and always returns a CheckInfo prepopulated with static data.
func (m *MockClient) Check(ctx context.Context) (*CheckInfo, error) {
	return &CheckInfo{
		Version:        "v13.5.0",
		Revision:       "deadbeef",
		APIVersion:     "v4",
		RedisReachable: true,
	}, nil
}

// PreReceive does nothing and always return true.
func (m *MockClient) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	return true, nil
}

// PostReceive does nothing and always returns true.
func (m *MockClient) PostReceive(ctx context.Context, glRepository, glID, changes string, gitPushOptions ...string) (bool, []PostReceiveMessage, error) {
	return true, nil, nil
}
