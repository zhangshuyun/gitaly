package interceptor

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

type Cache interface {
	InvalidateRepo(repo *gitalypb.Repository) error
}

// Invalidator will invalidate any mutating RPC that targets a repository
func Invalidator(c Cache, reg *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mInfo, err := reg.LookupMethod(info.FullMethod)
		if err != nil {
			logrus.Errorf("unable to lookup method information for %+v", info)
		}

		peeker := &StreamPeeker{ServerStream: ss}

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
			logrus.Errorf("cache invalidator interceptor unexpected operation type: %s", op)
		}

		return handler(srv, peeker)
	}
}

type StreamPeeker struct {
	grpc.ServerStream

	peeked    bool        // did you peek?
	peekedMsg interface{} // what did you peek?
	peekedErr error       // what did you screw up when you peeked?
}

// PeekMsg will peek one message into the stream to obtain the client's first
// request. This peeked message will be made available again when the
// StreamPeeker's RecvMsg method is invoked.
func (sp *StreamPeeker) PeekReq() (proto.Message, error) {
	if sp.peeked {
		return nil, errors.New("already peeked")
	}
	sp.peeked = true
	sp.peekedErr = sp.ServerStream.RecvMsg(sp.peekedMsg)

	pbMsg, ok := sp.peekedMsg.(proto.Message)
	if !ok {
		return nil, errors.New("peeked message is not protobuf")
	}

	return pbMsg, sp.peekedErr
}

func (sp *StreamPeeker) RecvMsg(m interface{}) error {
	if sp.peeked {
		sp.peeked = false
		m = sp.peekedMsg
		return sp.peekedErr
	}

	return sp.RecvMsg(m)
}
