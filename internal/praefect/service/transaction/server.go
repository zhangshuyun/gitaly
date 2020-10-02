package transaction

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type Server struct {
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
	if err != nil {
		switch {
		case errors.Is(err, transactions.ErrNotFound):
			return nil, helper.ErrNotFound(err)
		case errors.Is(err, transactions.ErrTransactionCanceled):
			return nil, helper.DecorateError(codes.Canceled, err)
		case errors.Is(err, transactions.ErrTransactionVoteFailed):
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_ABORT,
			}, nil
		default:
			return nil, helper.ErrInternal(err)
		}
	}

	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}
