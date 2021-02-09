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

// String returns the hexadecimal string representation of the vote.
func (v vote) String() string {
	return hex.EncodeToString(v[:])
}

// subtransaction is a single session where voters are voting for a certain outcome.
type subtransaction struct {
	doneCh chan interface{}

	threshold uint

	lock         sync.RWMutex
	votersByNode map[string]*Voter
	voteCounts   map[vote]uint
	isDone       bool
}

func newSubtransaction(voters []Voter, threshold uint) (*subtransaction, error) {
	votersByNode := make(map[string]*Voter, len(voters))
	for _, voter := range voters {
		voter := voter // rescope loop variable
		votersByNode[voter.Name] = &voter
	}

	return &subtransaction{
		doneCh:       make(chan interface{}),
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

	if !t.isDone {
		t.isDone = true
		close(t.doneCh)
	}
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

	if !t.isDone {
		t.isDone = true
		close(t.doneCh)
	}

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
	if voter.vote != nil {
		return fmt.Errorf("node already cast a vote: %q", node)
	}

	switch voter.result {
	case VoteUndecided:
		// Happy case, we can still cast a vote.
		break
	case VoteCanceled:
		return ErrTransactionCanceled
	case VoteStopped:
		return ErrTransactionStopped
	default:
		// Because we didn't vote yet, we know that the node cannot be
		// either in VoteCommitted or VoteFailed state.
		return fmt.Errorf("voter is in invalid state %d: %q", voter.result, node)
	}

	voter.vote = &vote

	t.voteCounts[vote] += voter.Votes

	// Update voter states to reflect the new vote counts. Before quorum is
	// reached, this function will check whether the threshold was reached
	// and, if so, update all voters which have already cast a vote. After
	// quorum was reached, it will only update the currently voting node.
	t.updateVoterStates()

	if t.mustSignalVoters() {
		t.isDone = true
		close(t.doneCh)
	}

	return nil
}

// updateVoterStates updates undecided voters. Voters are updated either as
// soon as quorum was reached or alternatively when all votes were cast.
func (t *subtransaction) updateVoterStates() {
	var majorityVote *vote
	for v, voteCount := range t.voteCounts {
		if voteCount >= t.threshold {
			v := v
			majorityVote = &v
			break
		}
	}

	allVotesCast := true
	for _, voter := range t.votersByNode {
		if voter.vote == nil {
			allVotesCast = false
			break
		}
	}

	// We need to adjust voter states either when quorum was reached or
	// when all votes were cast. If all votes were cast without reaching
	// quorum, we set all voters into VoteFailed state.
	if majorityVote == nil && !allVotesCast {
		return
	}

	// Update all voters which have cast a vote and which are not
	// undecided. We mustn't change any voters which did decide on an
	// outcome already as they may have already committed or aborted their
	// action.
	for _, voter := range t.votersByNode {
		if voter.result != VoteUndecided {
			continue
		}

		if voter.vote == nil || majorityVote == nil {
			if allVotesCast {
				voter.result = VoteFailed
			}
			continue
		}

		if *voter.vote == *majorityVote {
			voter.result = VoteCommitted
		} else {
			voter.result = VoteFailed
		}
	}
}

// mustSignalVoters determines whether we need to signal voters. Signalling may
// only happen once, so we need to make sure that either we just crossed the
// threshold or that nobody else did and no more votes are missing.
func (t *subtransaction) mustSignalVoters() bool {
	// If somebody else already notified voters, then we mustn't do so
	// again.
	if t.isDone {
		return false
	}

	// Check if any node has reached the threshold. If it did, then we need
	// to signal voters.
	for _, voteCount := range t.voteCounts {
		if voteCount >= t.threshold {
			return true
		}
	}

	// The threshold wasn't reached by any node yet. If there are missing
	// votes, then we cannot notify yet as any remaining nodes may cause us
	// to reach quorum.
	for _, voter := range t.votersByNode {
		if voter.vote == nil {
			return false
		}
	}

	// Otherwise we know that all votes are in and that no quorum was
	// reached. We thus need to notify callers of the failed vote as the
	// last node which has cast its vote.
	return true
}

func (t *subtransaction) collectVotes(ctx context.Context, node string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
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
	case VoteCommitted:
		// Happy case, we are part of the quorum.
		return nil
	case VoteFailed:
		if voter.vote == nil {
			return fmt.Errorf("%w: did not cast a vote", ErrTransactionFailed)
		}
		return fmt.Errorf("%w: got %d/%d votes for %v", ErrTransactionFailed,
			t.voteCounts[*voter.vote], t.threshold, *voter.vote)
	case VoteCanceled:
		// It may happen that the vote was cancelled or stopped just after majority was
		// reached. In that case, the node's state is now VoteCanceled/VoteStopped, so we
		// have to return an error here.
		return ErrTransactionCanceled
	case VoteStopped:
		return ErrTransactionStopped
	case VoteUndecided:
		// We shouldn't ever be undecided if the caller correctly calls
		// `vote()` before calling `collectVotes()` as this node
		// would've cast a vote in that case.
		return fmt.Errorf("voter is in undecided state: %q", node)
	default:
		return fmt.Errorf("voter is in invalid state %d: %q", voter.result, node)
	}
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
