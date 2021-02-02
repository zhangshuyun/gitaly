package transaction

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
		case errors.Is(err, transactions.ErrTransactionStopped):
			return &gitalypb.VoteTransactionResponse{
				State: gitalypb.VoteTransactionResponse_STOP,
			}, nil
		case errors.Is(err, transactions.ErrTransactionFailed):
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

// StopTransaction is called by a client who wants to gracefully stop a
// transaction. All voters waiting for quorum will be stopped and new votes
// will not get accepted anymore. It is fine to call this RPC multiple times on
// the same transaction.
func (s *Server) StopTransaction(ctx context.Context, in *gitalypb.StopTransactionRequest) (*gitalypb.StopTransactionResponse, error) {
	if err := s.txMgr.StopTransaction(ctx, in.TransactionId); err != nil {
		switch {
		case errors.Is(err, transactions.ErrNotFound):
			return nil, helper.ErrNotFound(err)
		case errors.Is(err, transactions.ErrTransactionCanceled):
			return nil, helper.DecorateError(codes.Canceled, err)
		case errors.Is(err, transactions.ErrTransactionStopped):
			return &gitalypb.StopTransactionResponse{}, nil
		default:
			return nil, helper.ErrInternal(err)
		}
	}

	return &gitalypb.StopTransactionResponse{}, nil
}
