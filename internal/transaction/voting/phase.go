package voting

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Phase is the transactional phase a given vote can be cast on.
type Phase int

const (
	// UnknownPhase is the default value. It should not be used.
	UnknownPhase = Phase(iota)
	// Prepared is the prepratory phase. The data that is about to change is locked for
	// concurrent modification, but changes have not yet been written to disk.
	Prepared
	// Committed is the committing phase. Data has been committed to disk and will be visible
	// in all subsequent requests.
	Committed
)

// ToProto converts the phase into its Protobuf enum. This function panics if called with an
// invalid phase.
func (p Phase) ToProto() gitalypb.VoteTransactionRequest_Phase {
	switch p {
	case UnknownPhase:
		return gitalypb.VoteTransactionRequest_UNKNOWN_PHASE
	case Prepared:
		return gitalypb.VoteTransactionRequest_PREPARED_PHASE
	case Committed:
		return gitalypb.VoteTransactionRequest_COMMITTED_PHASE
	default:
		panic(fmt.Sprintf("unknown phase %q", p))
	}
}
