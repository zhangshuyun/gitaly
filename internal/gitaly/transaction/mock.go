package transaction

import (
	"context"
	"errors"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
)

// MockManager is a mock Manager for use in tests.
type MockManager struct {
	VoteFn func(context.Context, txinfo.Transaction, voting.Vote, voting.Phase) error
	StopFn func(context.Context, txinfo.Transaction) error
}

// Vote calls the MockManager's Vote function, if set. Otherwise, it returns an error.
func (m *MockManager) Vote(ctx context.Context, tx txinfo.Transaction, vote voting.Vote, phase voting.Phase) error {
	if m.VoteFn == nil {
		return errors.New("mock does not implement Vote function")
	}
	return m.VoteFn(ctx, tx, vote, phase)
}

// Stop calls the MockManager's Stop function, if set. Otherwise, it returns an error.
func (m *MockManager) Stop(ctx context.Context, tx txinfo.Transaction) error {
	if m.StopFn == nil {
		return errors.New("mock does not implement Stop function")
	}
	return m.StopFn(ctx, tx)
}

// PhasedVote is used to keep track of votes and the phase they were cast in.
type PhasedVote struct {
	Vote  voting.Vote
	Phase voting.Phase
}

// TrackingManager is a transaction manager which tracks all votes. Voting functions never return
// an error.
type TrackingManager struct {
	MockManager

	votesLock sync.Mutex
	votes     []PhasedVote
}

// NewTrackingManager creates a new TrackingManager which is ready for use.
func NewTrackingManager() *TrackingManager {
	manager := &TrackingManager{}

	manager.VoteFn = func(_ context.Context, _ txinfo.Transaction, vote voting.Vote, phase voting.Phase) error {
		manager.votesLock.Lock()
		defer manager.votesLock.Unlock()
		manager.votes = append(manager.votes, PhasedVote{Vote: vote, Phase: phase})
		return nil
	}

	return manager
}

// Votes returns a copy of all votes which have been cast.
func (m *TrackingManager) Votes() []PhasedVote {
	m.votesLock.Lock()
	defer m.votesLock.Unlock()

	votes := make([]PhasedVote, len(m.votes))
	copy(votes, m.votes)

	return votes
}

// Reset resets all votes which have been recorded up to this point.
func (m *TrackingManager) Reset() {
	m.votesLock.Lock()
	defer m.votesLock.Unlock()
	m.votes = nil
}
