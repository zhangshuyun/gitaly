package transaction

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type Server struct {
	*gitalypb.UnimplementedRefTransactionServer
	txMgr *transactions.Manager
}

func NewServer(txMgr *transactions.Manager) gitalypb.RefTransactionServer {
	return &Server{
		txMgr: txMgr,
	}
}

// VoteTransaction is called by a client who's casting a vote on a reference
// transaction, blocking until a vote across all participating nodes has been
// completed.
func (s *Server) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	err := s.txMgr.VoteTransaction(ctx, in.TransactionId, in.Node, in.ReferenceUpdatesHash)
	return transactions.VoteResponseFor(err)
}

// StopTransaction is called by a client who wants to gracefully stop a
// transaction. All voters waiting for quorum will be stopped and new votes
// will not get accepted anymore. It is fine to call this RPC multiple times on
// the same transaction.
func (s *Server) StopTransaction(ctx context.Context, in *gitalypb.StopTransactionRequest) (*gitalypb.StopTransactionResponse, error) {
	err := s.txMgr.StopTransaction(ctx, in.TransactionId)
	return transactions.StopResponseFor(err)
}
