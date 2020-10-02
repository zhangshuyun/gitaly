package hook

import (
	"context"
	"crypto/sha1"
	"io"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
)

func (m *GitLabHookManager) ReferenceTransactionHook(ctx context.Context, env []string, stdin io.Reader) error {
	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternalf("reading stdin from request: %w", err)
	}
	hash := sha1.Sum(changes)

	if err := m.voteOnTransaction(ctx, hash[:], env); err != nil {
		return helper.ErrInternalf("error voting on transaction: %v", err)
	}

	return nil
}
