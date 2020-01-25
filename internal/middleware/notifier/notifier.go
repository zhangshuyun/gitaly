package notifier

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func shouldIgnore(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health")
}

// StreamNotifier bla
func StreamNotifier(gitlab config.GitlabRails, reg *protoregistry.Registry) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if shouldIgnore(info.FullMethod) {
			return handler(srv, ss)
		}

		mInfo, err := reg.LookupMethod(info.FullMethod)
		if err != nil {
			logrus.WithError(err).Error("method lookup")
			return handler(srv, ss)
		}

		if mInfo.Scope != protoregistry.ScopeRepository || mInfo.Operation == protoregistry.OpAccessor {
			return handler(srv, ss)
		}

		handler, callback := notifyChange(gitlab, mInfo, handler)
		peeker := newStreamPeeker(ss, callback)
		return handler(srv, peeker)
	}
}

const contentType = "application/json"

// UnaryNotifier bla
func UnaryNotifier(gitlab config.GitlabRails, reg *protoregistry.Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		if shouldIgnore(info.FullMethod) {
			return handler(ctx, req)
		}

		mInfo, err := reg.LookupMethod(info.FullMethod)
		if err != nil {
			logrus.WithError(err).Error("method lookup")
		}

		if mInfo.Scope != protoregistry.ScopeRepository || mInfo.Operation == protoregistry.OpAccessor {
			return handler(ctx, req)
		}

		pbReq, ok := req.(proto.Message)
		if !ok {
			logrus.Errorf("expected protobuf message but got %T", req)
			return handler(ctx, req)
		}

		target, err := mInfo.TargetRepo(pbReq)
		if err != nil {
			logrus.WithError(err).Error("expected target repository")
			return handler(ctx, req)
		}

		// TODO: notify gitlab-rails of start of write

		// wrap the handler to ensure the lease is always ended
		return func() (resp interface{}, err error) {
			defer notifyAPI(gitlab, target)
			return handler(ctx, req)
		}()
	}
}

func notifyAPI(gitlab config.GitlabRails, repo *gitalypb.Repository) {
	data, err := proto.Marshal(repo)
	if err != nil {
		logrus.WithError(err).Error("marshal repo")
		return
	}

	body := strings.NewReader(fmt.Sprintf(
		`{"payload":"%s"}`,
		base64.StdEncoding.EncodeToString(data),
	))

	notifyResp, err := http.Post(
		gitlab.URL+`/api/v4/internal/praefect/finish-write`,
		contentType,
		body,
	)
	if err != nil {
		logrus.WithError(err).Error("http post")
		return
	}

	notifyResp.Body.Close() // Important! Prevent resource leak.
}

type recvMsgCallback func(interface{}, error)

func notifyChange(gitlab config.GitlabRails, mInfo protoregistry.MethodInfo, handler grpc.StreamHandler) (grpc.StreamHandler, recvMsgCallback) {
	var repo struct {
		sync.Mutex
		*gitalypb.Repository
	}

	// ensures that the lease ender is invoked after the original handler
	wrappedHandler := func(srv interface{}, stream grpc.ServerStream) error {
		defer func() {
			repo.Lock()
			defer repo.Unlock()

			if repo.Repository == nil {
				return
			}
			notifyAPI(gitlab, repo.Repository)
		}()
		return handler(srv, stream)
	}

	// starts the cache lease and sets the lease ender iff the request's target
	// repository can be determined from the first request message
	peekerCallback := func(firstReq interface{}, err error) {
		if err != nil {
			logrus.WithError(err).Error("peeker callback")
			return
		}

		pbFirstReq, ok := firstReq.(proto.Message)
		if !ok {
			logrus.WithError(fmt.Errorf("cache invalidation expected protobuf request, but got %T", firstReq)).Error("cast to proto")
			return
		}

		target, err := mInfo.TargetRepo(pbFirstReq)
		if err != nil {
			logrus.WithError(err).Error("get target")
			return
		}

		// TODO: notify gitlab-rails of start of write

		repo.Lock()
		defer repo.Unlock()

		repo.Repository = target
	}

	return wrappedHandler, peekerCallback
}

// streamPeeker allows a stream interceptor to insert peeking logic to perform
// an action when the first RecvMsg
type streamPeeker struct {
	grpc.ServerStream

	// onFirstRecvCallback is called the first time the server stream's RecvMsg
	// is invoked. It passes the results of the stream's RecvMsg as the
	// callback's parameters.
	onFirstRecvOnce     sync.Once
	onFirstRecvCallback recvMsgCallback
}

// newStreamPeeker returns a wrapped stream that allows a callback to be called
// on the first invocation of RecvMsg.
func newStreamPeeker(stream grpc.ServerStream, callback recvMsgCallback) grpc.ServerStream {
	return &streamPeeker{
		ServerStream:        stream,
		onFirstRecvCallback: callback,
	}
}

// RecvMsg overrides the embedded grpc.ServerStream's method of the same name so
// that the callback is called on the first call.
func (sp *streamPeeker) RecvMsg(m interface{}) error {
	err := sp.ServerStream.RecvMsg(m)
	sp.onFirstRecvOnce.Do(func() { sp.onFirstRecvCallback(m, err) })
	return err
}
