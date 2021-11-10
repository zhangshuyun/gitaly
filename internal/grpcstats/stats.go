package grpcstats

import (
	"context"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/stats"
)

// PayloadBytes implements stats.Handler and tracks amount of bytes received and send by gRPC service
// for each method call. The information about statistics is added into the context and can be
// extracted with payloadBytesStatsFromContext.
type PayloadBytes struct{}

type payloadBytesStatsKey struct{}

// HandleConn exists to satisfy gRPC stats.Handler.
func (s *PayloadBytes) HandleConn(context.Context, stats.ConnStats) {}

// TagConn exists to satisfy gRPC stats.Handler. We don't gather connection level stats
// and are thus not currently using it.
func (s *PayloadBytes) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleRPC implements per-RPC tracing and stats instrumentation.
func (s *PayloadBytes) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	switch st := rs.(type) {
	case *stats.InPayload:
		bytesStats := ctx.Value(payloadBytesStatsKey{}).(*PayloadBytesStats)
		bytesStats.InPayloadBytes += int64(st.Length)
	case *stats.OutPayload:
		bytesStats := ctx.Value(payloadBytesStatsKey{}).(*PayloadBytesStats)
		bytesStats.OutPayloadBytes += int64(st.Length)
	}
}

// TagRPC initializes context with an RPC specific stats collector.
// The returned context will be used in method invocation as is passed into HandleRPC.
func (s *PayloadBytes) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, payloadBytesStatsKey{}, new(PayloadBytesStats))
}

// PayloadBytesStats contains info about bytes received and sent by the gRPC method.
type PayloadBytesStats struct {
	// InPayloadBytes amount of bytes received.
	InPayloadBytes int64
	// OutPayloadBytes amount of bytes sent.
	OutPayloadBytes int64
}

// Fields returns logging info.
func (s *PayloadBytesStats) Fields() logrus.Fields {
	return logrus.Fields{
		"grpc.request.payload_bytes":  s.InPayloadBytes,
		"grpc.response.payload_bytes": s.OutPayloadBytes,
	}
}

// FieldsProducer extracts stats info from the context and returns it as a logging fields.
func FieldsProducer(ctx context.Context) logrus.Fields {
	payloadBytesStats := payloadBytesStatsFromContext(ctx)
	if payloadBytesStats != nil {
		return payloadBytesStats.Fields()
	}
	return nil
}

func payloadBytesStatsFromContext(ctx context.Context) *PayloadBytesStats {
	v, ok := ctx.Value(payloadBytesStatsKey{}).(*PayloadBytesStats)
	if !ok {
		return nil
	}
	return v
}
