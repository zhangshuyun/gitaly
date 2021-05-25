package hook

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
)

func (m *GitLabHookManager) Check(ctx context.Context) (*gitlab.CheckInfo, error) {
	return m.gitlabClient.Check(ctx)
}
