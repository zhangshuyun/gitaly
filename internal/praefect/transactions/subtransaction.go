package transactions

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
)

// VoteResult represents the outcome of a transaction for a single voter.
type VoteResult int

const (
	// VoteUndecided means that the voter either didn't yet show up or that
	// the vote couldn't yet be decided due to there being no majority yet.
	VoteUndecided VoteResult = iota
	// VoteCommitted means that the voter committed his vote.
	VoteCommitted
	// VoteFailed means that the voter has failed the vote because a
	// majority of nodes has elected a different result.
	VoteFailed
	// VoteCanceled means that the transaction was cancelled.
	VoteCanceled
	// VoteStopped means that the transaction was gracefully stopped.
	VoteStopped
)

type vote [sha1.Size]byte

func voteFromHash(hash []byte) (vote, error) {
	var vote vote

	if len(hash) != sha1.Size {
		return vote, fmt.Errorf("invalid voting hash: %q", hash)
	}

	copy(vote[:], hash)
	return vote, nil
}

func (v vote) isEmpty() bool {
	return v == vote{}
}

// String returns the hexadecimal string representation of the vote.
func (v vote) String() string {
	return hex.EncodeToString(v[:])
}

// subtransaction is a single session where voters are voting for a certain outcome.
type subtransaction struct {
	doneCh   chan interface{}
	cancelCh chan interface{}
	stopCh   chan interface{}

	threshold uint

	lock         sync.RWMutex
	votersByNode map[string]*Voter
	voteCounts   map[vote]uint
}

func newSubtransaction(voters []Voter, threshold uint) (*subtransaction, error) {
	votersByNode := make(map[string]*Voter, len(voters))
	for _, voter := range voters {
		voter := voter // rescope loop variable
		votersByNode[voter.Name] = &voter
	}

	return &subtransaction{
		doneCh:       make(chan interface{}),
		cancelCh:     make(chan interface{}),
		stopCh:       make(chan interface{}),
		threshold:    threshold,
		votersByNode: votersByNode,
		voteCounts:   make(map[vote]uint, len(voters)),
	}, nil
}

func (t *subtransaction) cancel() {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, voter := range t.votersByNode {
		// If a voter didn't yet show up or is still undecided, we need
		// to mark it as failed so it won't get the idea of committing
		// the transaction at a later point anymore.
		if voter.result == VoteUndecided {
			voter.result = VoteCanceled
		}
	}

	close(t.cancelCh)
}

func (t *subtransaction) stop() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	for _, voter := range t.votersByNode {
		switch voter.result {
		case VoteCanceled:
			// If the vote was canceled already, we cannot stop it.
			return ErrTransactionCanceled
		case VoteStopped:
			// Similar if the vote was stopped already.
			return ErrTransactionStopped
		case VoteUndecided:
			// Undecided voters will get stopped, ...
			voter.result = VoteStopped
		case VoteCommitted, VoteFailed:
			// ... while decided voters cannot be changed anymore.
			continue
		}
	}

	close(t.stopCh)
	return nil
}

func (t *subtransaction) state() map[string]VoteResult {
	t.lock.Lock()
	defer t.lock.Unlock()

	results := make(map[string]VoteResult, len(t.votersByNode))
	for node, voter := range t.votersByNode {
		results[node] = voter.result
	}

	return results
}

func (t *subtransaction) vote(node string, hash []byte) error {
	vote, err := voteFromHash(hash)
	if err != nil {
		return err
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	// Cast our vote. In case the node doesn't exist or has already cast a
	// vote, we need to abort.
	voter, ok := t.votersByNode[node]
	if !ok {
		return fmt.Errorf("invalid node for transaction: %q", node)
	}
	if !voter.vote.isEmpty() {
		return fmt.Errorf("node already cast a vote: %q", node)
	}
	voter.vote = vote

	oldCount := t.voteCounts[vote]
	newCount := oldCount + voter.Votes
	t.voteCounts[vote] = newCount

	// If the threshold was reached before already, we mustn't try to
	// signal the other voters again.
	if oldCount >= t.threshold {
		return nil
	}

	// If we've just crossed the threshold, signal all voters that the
	// voting has concluded.
	if newCount >= t.threshold {
		close(t.doneCh)
		return nil
	}

	// If any other vote has already reached the threshold, we mustn't try
	// to notify voters again.
	for _, count := range t.voteCounts {
		if count >= t.threshold {
			return nil
		}
	}

	// If any of the voters didn't yet cast its vote, we need to wait for
	// them.
	for _, voter := range t.votersByNode {
		if voter.vote.isEmpty() {
			return nil
		}
	}

	// Otherwise, signal voters that all votes were gathered.
	close(t.doneCh)
	return nil
}

func (t *subtransaction) collectVotes(ctx context.Context, node string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.cancelCh:
		return ErrTransactionCanceled
	case <-t.stopCh:
		return ErrTransactionStopped
	case <-t.doneCh:
		break
	}

	t.lock.RLock()
	defer t.lock.RUnlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return fmt.Errorf("invalid node for transaction: %q", node)
	}

	switch voter.result {
	case VoteUndecided:
		// Happy case, no decision was yet made.
	case VoteCanceled:
		// It may happen that the vote was cancelled or stopped just after majority was
		// reached. In that case, the node's state is now VoteCanceled/VoteStopped, so we
		// have to return an error here.
		return ErrTransactionCanceled
	case VoteStopped:
		return ErrTransactionStopped
	default:
		return fmt.Errorf("voter is in invalid state %d: %q", voter.result, node)
	}

	// See if our vote crossed the threshold. If not, then we know we
	// cannot have won as the transaction is being wrapped up already and
	// shouldn't accept any additional votes.
	if t.voteCounts[voter.vote] < t.threshold {
		voter.result = VoteFailed
		return fmt.Errorf("%w: got %d/%d votes for %v", ErrTransactionFailed,
			t.voteCounts[voter.vote], t.threshold, voter.vote)
	}

	voter.result = VoteCommitted
	return nil
}

func (t *subtransaction) getResult(node string) (VoteResult, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	voter, ok := t.votersByNode[node]
	if !ok {
		return VoteCanceled, fmt.Errorf("invalid node for transaction: %q", node)
	}

	return voter.result, nil
}
