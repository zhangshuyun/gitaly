package transactions

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestSubtransaction_cancel(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	s.cancel()

	require.True(t, s.isDone)
	require.Equal(t, VoteCanceled, s.votersByNode["1"].result)
	require.Equal(t, VoteCommitted, s.votersByNode["2"].result)
	require.Equal(t, VoteFailed, s.votersByNode["3"].result)
	require.Equal(t, VoteCanceled, s.votersByNode["4"].result)
}

func TestSubtransaction_stop(t *testing.T) {
	t.Run("stop of ongoing transaction", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
		}, 1)
		require.NoError(t, err)

		require.NoError(t, s.stop())

		require.True(t, s.isDone)
		require.Equal(t, VoteStopped, s.votersByNode["1"].result)
		require.Equal(t, VoteCommitted, s.votersByNode["2"].result)
		require.Equal(t, VoteFailed, s.votersByNode["3"].result)
	})

	t.Run("stop of canceled transaction fails", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
			{Name: "4", Votes: 1, result: VoteCanceled},
		}, 1)
		require.NoError(t, err)

		require.Equal(t, s.stop(), ErrTransactionCanceled)
		require.False(t, s.isDone)
	})

	t.Run("stop of stopped transaction fails", func(t *testing.T) {
		s, err := newSubtransaction([]Voter{
			{Name: "1", Votes: 1, result: VoteUndecided},
			{Name: "2", Votes: 1, result: VoteCommitted},
			{Name: "3", Votes: 1, result: VoteFailed},
			{Name: "4", Votes: 1, result: VoteStopped},
		}, 1)
		require.NoError(t, err)

		require.Equal(t, s.stop(), ErrTransactionStopped)
		require.False(t, s.isDone)
	})
}

func TestSubtransaction_state(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	require.Equal(t, map[string]VoteResult{
		"1": VoteUndecided,
		"2": VoteCommitted,
		"3": VoteFailed,
		"4": VoteCanceled,
	}, s.state())
}

func TestSubtransaction_getResult(t *testing.T) {
	s, err := newSubtransaction([]Voter{
		{Name: "1", Votes: 1, result: VoteUndecided},
		{Name: "2", Votes: 1, result: VoteCommitted},
		{Name: "3", Votes: 1, result: VoteFailed},
		{Name: "4", Votes: 1, result: VoteCanceled},
	}, 1)
	require.NoError(t, err)

	for _, tc := range []struct {
		name           string
		expectedErr    error
		expectedResult VoteResult
	}{
		{name: "1", expectedResult: VoteUndecided},
		{name: "2", expectedResult: VoteCommitted},
		{name: "3", expectedResult: VoteFailed},
		{name: "4", expectedResult: VoteCanceled},
		{name: "missingNode", expectedResult: VoteCanceled, expectedErr: errors.New("invalid node for transaction: \"missingNode\"")},
	} {
		result, err := s.getResult(tc.name)
		require.Equal(t, tc.expectedErr, err)
		require.Equal(t, tc.expectedResult, result)
	}
}

func TestSubtransaction_vote(t *testing.T) {
	var zeroVote vote
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")

	for _, tc := range []struct {
		desc               string
		voters             []Voter
		threshold          uint
		voterName          string
		vote               vote
		expectedVoterState []Voter
		expectedVoteCounts map[vote]uint
		expectedErr        error
	}{
		{
			desc: "single voter doing final vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 1,
			},
		},
		{
			desc: "single voter trying to vote twice",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 1,
			},
			expectedErr: errors.New("node already cast a vote: \"1\""),
		},
		{
			desc: "single voter can cast all-zeroes vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
			},
			threshold: 1,
			voterName: "1",
			vote:      zeroVote,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &zeroVote},
			},
			expectedVoteCounts: map[vote]uint{
				zeroVote: 1,
			},
		},
		{
			desc: "single voter trying to vote on canceled transaction",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCanceled},
			},
			expectedVoteCounts: map[vote]uint{},
			expectedErr:        ErrTransactionCanceled,
		},
		{
			desc: "single voter trying to vote on stopped transaction",
			voters: []Voter{
				{Name: "1", Votes: 1, result: VoteStopped},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteStopped},
			},
			expectedVoteCounts: map[vote]uint{},
			expectedErr:        ErrTransactionStopped,
		},
		{
			desc: "multiple voters doing final vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold: 1,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteCommitted, vote: &voteA},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 3,
			},
		},
		{
			desc: "multiple voters with missing votes do not commit",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold: 3,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 2,
			},
		},
		{
			desc: "multiple voters not reaching quorum fail transaction",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold: 3,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteFailed, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteFailed, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteFailed, vote: &voteB},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 2,
				voteB: 1,
			},
		},
		{
			desc: "multiple voters reaching quorum with partial failure",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold: 2,
			voterName: "1",
			vote:      voteA,
			expectedVoterState: []Voter{
				{Name: "1", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "2", Votes: 1, result: VoteCommitted, vote: &voteA},
				{Name: "3", Votes: 1, result: VoteFailed, vote: &voteB},
			},
			expectedVoteCounts: map[vote]uint{
				voteA: 2,
				voteB: 1,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			s, err := newSubtransaction(tc.voters, tc.threshold)
			require.NoError(t, err)

			voteCounts := make(map[vote]uint)
			for _, voter := range tc.voters {
				if voter.vote != nil {
					voteCounts[*voter.vote] += voter.Votes
				}
			}
			s.voteCounts = voteCounts

			expectedVoterState := make(map[string]*Voter)
			for _, voter := range tc.expectedVoterState {
				voter := voter
				expectedVoterState[voter.Name] = &voter
			}

			require.Equal(t, tc.expectedErr, s.vote(tc.voterName, tc.vote[:]))
			require.Equal(t, expectedVoterState, s.votersByNode)
			require.Equal(t, tc.expectedVoteCounts, s.voteCounts)
		})
	}
}

func TestSubtransaction_mustSignalVoters(t *testing.T) {
	voteA := newVote(t, "a")
	voteB := newVote(t, "b")
	voteC := newVote(t, "c")

	for _, tc := range []struct {
		desc       string
		voters     []Voter
		threshold  uint
		isDone     bool
		mustSignal bool
	}{
		{
			desc: "single voter with vote",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
			},
			threshold:  1,
			mustSignal: true,
		},
		{
			desc: "single voter with missing vote",
			voters: []Voter{
				{Name: "1", Votes: 1},
			},
			threshold:  1,
			mustSignal: false,
		},
		{
			desc: "multiple agreeing voters",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold:  1,
			mustSignal: true,
		},
		{
			desc: "multiple disagreeing voters not reaching threshold",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1, vote: &voteB},
				{Name: "3", Votes: 1, vote: &voteC},
			},
			threshold:  3,
			mustSignal: true,
		},
		{
			desc: "multiple disagreeing voters reaching threshold",
			voters: []Voter{
				{Name: "1", Votes: 1, vote: &voteA},
				{Name: "2", Votes: 1, vote: &voteB},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold:  2,
			mustSignal: true,
		},
		{
			desc: "multiple voters reach quorum with with missing votes",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteA},
				{Name: "3", Votes: 1, vote: &voteA},
			},
			threshold:  2,
			mustSignal: true,
		},
		{
			desc: "multiple voters do not reach quorum with missing votes",
			voters: []Voter{
				{Name: "1", Votes: 1},
				{Name: "2", Votes: 1, vote: &voteB},
				{Name: "3", Votes: 1, vote: &voteB},
			},
			threshold:  3,
			mustSignal: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			s, err := newSubtransaction(tc.voters, tc.threshold)
			require.NoError(t, err)

			voteCounts := make(map[vote]uint)
			for _, voter := range tc.voters {
				if voter.vote != nil {
					voteCounts[*voter.vote] += voter.Votes
				}
			}

			s.voteCounts = voteCounts
			s.isDone = tc.isDone

			require.Equal(t, tc.mustSignal, s.mustSignalVoters())
		})
	}
}

func TestSubtransaction_race(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	voters := make([]Voter, 1000)
	for i := range voters {
		voters[i] = Voter{Name: fmt.Sprintf("%d", i), Votes: 1}
	}

	voteA := newVote(t, "a")

	for _, threshold := range []uint{1, 100, 500, 1000} {
		t.Run(fmt.Sprintf("threshold: %d", threshold), func(t *testing.T) {
			s, err := newSubtransaction(voters, threshold)
			require.NoError(t, err)

			var wg sync.WaitGroup
			for _, voter := range voters {
				wg.Add(1)
				go func(voter Voter) {
					defer wg.Done()

					result, err := s.getResult(voter.Name)
					require.NoError(t, err)
					require.Equal(t, VoteUndecided, result)

					require.NoError(t, s.vote(voter.Name, voteA[:]))
					require.NoError(t, s.collectVotes(ctx, voter.Name))

					result, err = s.getResult(voter.Name)
					require.NoError(t, err)
					require.Equal(t, VoteCommitted, result)
				}(voter)
			}

			wg.Wait()
		})
	}
}

func newVote(t *testing.T, s string) vote {
	hash := sha1.Sum([]byte(s))
	vote, err := voteFromHash(hash[:])
	require.NoError(t, err)
	return vote
}
