package hook

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/internal/git"
)

const (
	// forceDeletionPrefix is the prefix of a queued reference transaction which deletes a
	// reference without checking its current value.
	forceDeletionPrefix = "0000000000000000000000000000000000000000 0000000000000000000000000000000000000000 "
)

func (m *GitLabHookManager) ReferenceTransactionHook(ctx context.Context, state ReferenceTransactionState, env []string, stdin io.Reader) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return fmt.Errorf("extracting hooks payload: %w", err)
	}

	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return fmt.Errorf("reading stdin from request: %w", err)
	}

	// We're only voting in prepared state as this is the only stage in
	// Git's reference transaction which allows us to abort the
	// transaction.
	if state != ReferenceTransactionPrepared {
		return nil
	}

	// When deleting references, git has to delete them both in the packed-refs backend as well
	// as any loose refs -- if only the loose ref was deleted, it would potentially unshadow the
	// value contained in the packed-refs file and vice versa. As a result, git will create two
	// transactions when any ref exists in both backends: one session to force-delete all
	// existing refs in the packed-refs backend, and then one transaction to update all loose
	// refs. This is problematic for us, as our voting logic now requires all nodes to have the
	// same packed state, which we do not and cannot guarantee.
	//
	// We're lucky though and can fix this quite easily: git only needs to cope with unshadowing
	// refs when deleting loose refs, so it will only ever _delete_ refs from the packed-refs
	// backend and never _update_ any refs. And if such a force-deletion happens, the same
	// deletion will also get queued to the loose backend no matter whether the loose ref exists
	// or not given that it must be locked during the whole transaction. As such, we can easily
	// recognize those packed-refs cleanups: all queued ref updates are force deletions.
	//
	// The workaround is thus clear: we simply do not cast a vote on any reference transaction
	// which consists only of force-deletions -- the vote will instead only happen on the loose
	// backend transaction, which contains the full record of all refs which are to be updated.
	if isForceDeletionsOnly(bytes.NewReader(changes)) {
		return nil
	}

	hash := sha1.Sum(changes)

	if err := m.voteOnTransaction(ctx, hash[:], payload); err != nil {
		return fmt.Errorf("error voting on transaction: %w", err)
	}

	return nil
}

// isForceDeletionsOnly determines whether the given changes only consist of force-deletions.
func isForceDeletionsOnly(changes io.Reader) bool {
	scanner := bufio.NewScanner(changes)

	for scanner.Scan() {
		line := scanner.Bytes()

		if bytes.HasPrefix(line, []byte(forceDeletionPrefix)) {
			continue
		}

		return false
	}

	return true
}
