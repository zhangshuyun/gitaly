package repositoryhandler

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type RepositoryRequest interface {
	GetRepository() *gitalypb.Repository
}

func RepositoryTransactionUnaryInterceptor(transactions *repository.TransactionManager, registry *protoregistry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		mi, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return handler(ctx, req)
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
				if repoReq.GetRepository() != nil {

					logrus.WithField("repository", repoReq.GetRepository()).Info("trying to start new transaction")
					transactions.NewTransaction(transactionID, repoReq.GetRepository())
					logrus.WithField("repository", repoReq.GetRepository()).Info("started new transaction")
				}
			}

			defer transactions.Release(transactionID)

			md.Set("transaction_id", transactionID)
			ctx = metadata.NewIncomingContext(ctx, md)

			grpc.SetTrailer(ctx, metadata.New(map[string]string{"transaction_id": transactionID}))
		}

		return handler(ctx, req)
	}
}

func NewTransactionServerStream(ss grpc.ServerStream, methodInfo protoregistry.MethodInfo, transactions *repository.TransactionManager, transactionID string) TransactionServerStream {
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
	transactions  *repository.TransactionManager
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
		if ok && repoReq.GetRepository() != nil {
			logrus.WithField("repository", repoReq.GetRepository()).Info("trying to start new transaction")
			t.transactions.NewTransaction(t.transactionID, repoReq.GetRepository())
			logrus.WithField("repository", repoReq.GetRepository()).Info("started new transaction")
		}
	}

	t.ss.SetTrailer(metadata.Pairs("transaction_id", t.transactionID))

	return t.ss.RecvMsg(m)
}

// StreamInterceptor returns a Stream Interceptor
func RepositoryTransactionStreamInterceptor(transactions *repository.TransactionManager, registry *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mi, err := registry.LookupMethod(info.FullMethod)
		if err != nil {
			return handler(srv, stream)
		}

		if mi.Scope == protoregistry.ScopeRepository && mi.Operation == protoregistry.OpMutator {
			var transactionID string

			md, ok := metadata.FromIncomingContext(stream.Context())
			if ok && len(md.Get("transaction_id")) > 0 {
				transactionID = md.Get("transaction_id")[0]
			} else {
				transactionID = uuid.New().String()
			}

			defer transactions.Release(transactionID)

			return handler(srv, NewTransactionServerStream(stream, mi, transactions, transactionID))
		}

		return handler(srv, stream)
	}
}
