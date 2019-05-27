package interceptor

import (
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

// RepoCache is a cache that is able to invalidate parts of the cache
// pertinent to a specific repository.
type RepoCache interface {
	InvalidateRepo(repo *gitalypb.Repository) error
}

type RequestFactory interface {
	NewRequest() (proto.Message, error)
}

// StreamInvalidator will invalidate any mutating RPC that targets a repository
// in a gRPC stream based RPC
func StreamInvalidator(c RepoCache, reg *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mInfo, err := reg.LookupMethod(info.FullMethod)
		if err != nil {
			logrus.Errorf("unable to lookup method information for %+v", info)
		}

		peeker := &StreamPeeker{
			ServerStream: ss,
			reqFactory:   mInfo,
		}

		switch op := mInfo.Operation; op {
		case protoregistry.OpAccessor:
			break
		case protoregistry.OpMutator:
			peekedMsg, err := peeker.PeekReq()
			if err != nil {
				logrus.Errorf("cache invalidator interceptor unable to peek into stream: %s", err)
			}

			target, err := mInfo.TargetRepo(peekedMsg)
			if err != nil {
				logrus.Errorf("cache invalidator interceptor unable to find target repo: %s", err)
			}

			if err := c.InvalidateRepo(target); err != nil {
				logrus.Errorf("cache invalidator interceptor unable to invalidate repo: %s", err)
			}
		default:
			logrus.Errorf("cache invalidator interceptor unexpected operation type: %d", op)
		}

		return handler(srv, peeker)
	}
}

// StreamPeeker allows a stream interceptor the ability to peak into a stream
// without removing messages from the next interceptor/handler.
type StreamPeeker struct {
	grpc.ServerStream

	peeked    bool          // did you peek?
	peekedMsg proto.Message // what did you peek?
	peekedErr error         // what did you screw up when you peeked?

	reqFactory RequestFactory
}

// PeekMsg will peek one message into the stream to obtain the client's first
// request. This peeked message will be made available again when the
// StreamPeeker's RecvMsg method is invoked.
func (sp *StreamPeeker) PeekReq() (proto.Message, error) {
	if sp.peeked {
		return nil, errors.New("already peeked")
	}

	sp.peeked = true

	var err error
	sp.peekedMsg, err = sp.reqFactory.NewRequest()
	if err != nil {
		return nil, err
	}

	sp.peekedErr = sp.ServerStream.RecvMsg(sp.peekedMsg)
	log.Printf("👽: %#v", sp.peekedMsg)

	return sp.peekedMsg, sp.peekedErr
}

// RecvMsg overrides the embedded grpc.ServerStream's method of the same name.
// It transparently ensures that any peeked messages are forwarded to the stream
// as intended without the StreamPeeker.
func (sp *StreamPeeker) RecvMsg(m interface{}) error {
	if sp.peeked {
		sp.peeked = false
		m = sp.peekedMsg
		log.Printf("Forwarding peeked msg: %#v", sp.peekedMsg)

		mv := reflect.ValueOf(m)
		if mv.Kind() != reflect.Ptr || mv.IsNil() {
			return fmt.Errorf("receievd message of wrong type: %s", mv.Type())
		}
		mv.Elem().Set(reflect.ValueOf(sp.peekedMsg).Elem())

		log.Printf("🤖: %#v", m)

		return sp.peekedErr
	}

	return sp.ServerStream.RecvMsg(m)
}
