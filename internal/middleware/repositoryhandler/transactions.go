package repositoryhandler

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"

	"google.golang.org/grpc"
)

type RepositoryRequest interface {
	GetRepository() *gitalypb.Repository
}

func RepositoryTransactionUnaryInterceptor(transactions *repository.Transactions, registry *protoregistry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		mi, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return nil, err
		}

		if mi.Scope == protoregistry.ScopeRepository && mi.Operation == protoregistry.OpMutator {
			var transactionID string

			md, ok := metadata.FromIncomingContext(ctx)
			if ok && len(md.Get("transaction_id")) > 0 {
				transactionID = md.Get("transaction_id")[0]
			} else {
				transactionID = uuid.New().String()

			}

			if !transactions.TransactionStarted(transactionID) {
				repoReq, ok := req.(RepositoryRequest)
				if !ok {
					return nil, errors.New("not a repository request")
				}

				transactions.NewTransaction(transactionID, repoReq.GetRepository())
			}

			defer transactions.Unlock(transactionID)

			ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{"transaction_id": transactionID}))

			grpc.SetTrailer(ctx, metadata.New(map[string]string{"transaction_id": transactionID}))
		}

		res, err := handler(ctx, req)

		return res, err
	}
}

func NewTransactionServerStream(ss grpc.ServerStream, methodInfo protoregistry.MethodInfo, transactions *repository.Transactions, transactionID string) TransactionServerStream {
	return TransactionServerStream{
		transactionID: transactionID,
		ss:            ss,
		mi:            methodInfo,
		transactions:  transactions,
	}
}

type TransactionServerStream struct {
	transactionID string
	ss            grpc.ServerStream
	mi            protoregistry.MethodInfo
	transactions  *repository.Transactions
}

func (t TransactionServerStream) SetHeader(m metadata.MD) error {
	return t.ss.SetHeader(m)
}

func (t TransactionServerStream) SendHeader(m metadata.MD) error {
	return t.ss.SendHeader(m)
}

func (t TransactionServerStream) SetTrailer(m metadata.MD) {
	t.ss.SetTrailer(m)
}

func (t TransactionServerStream) Context() context.Context {
	return t.ss.Context()
}

func (t TransactionServerStream) SendMsg(m interface{}) error {
	return t.ss.SendMsg(m)
}

func (t TransactionServerStream) RecvMsg(m interface{}) error {
	if !t.transactions.TransactionStarted(t.transactionID) {
		fmt.Printf("Receiving Message: type=%s\n", reflect.TypeOf(m).String())
		repoReq, ok := m.(RepositoryRequest)
		if ok {
			t.transactions.NewTransaction(t.transactionID, repoReq.GetRepository())
		}
	}

	t.ss.SetTrailer(metadata.Pairs("transaction_id", t.transactionID))

	return t.ss.RecvMsg(m)
}

// StreamInterceptor returns a Stream Interceptor
func RepositoryTransactionServerInterceptor(transactions *repository.Transactions, registry *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()

		mi, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return err
		}

		if mi.Scope == protoregistry.ScopeRepository && mi.Operation == protoregistry.OpMutator {
			var transactionID string

			md, ok := metadata.FromIncomingContext(ctx)
			if ok && len(md.Get("transaction_id")) > 0 {
				transactionID = md.Get("transaction_id")[0]
			} else {
				transactionID = uuid.New().String()
			}

			defer transactions.Unlock(transactionID)

			return handler(srv, NewTransactionServerStream(stream, mi, transactions, transactionID))
		}

		return handler(srv, stream)
	}
}
