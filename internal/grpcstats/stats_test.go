package grpcstats

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc/stats"
)

func TestPayloadBytes_TagRPC(t *testing.T) {
	ctx := testhelper.Context(t)
	ctx = (&PayloadBytes{}).TagRPC(ctx, nil)
	require.Equal(t,
		logrus.Fields{"grpc.request.payload_bytes": int64(0), "grpc.response.payload_bytes": int64(0)},
		FieldsProducer(ctx),
	)
}

func TestPayloadBytes_HandleRPC(t *testing.T) {
	ctx := testhelper.Context(t)
	handler := &PayloadBytes{}
	ctx = handler.TagRPC(ctx, nil)
	handler.HandleRPC(ctx, nil)            // sanity check we don't fail anything
	handler.HandleRPC(ctx, &stats.Begin{}) // sanity check we don't fail anything
	handler.HandleRPC(ctx, &stats.InPayload{Length: 42})
	require.Equal(t,
		logrus.Fields{"grpc.request.payload_bytes": int64(42), "grpc.response.payload_bytes": int64(0)},
		FieldsProducer(ctx),
	)
	handler.HandleRPC(ctx, &stats.OutPayload{Length: 24})
	require.Equal(t,
		logrus.Fields{"grpc.request.payload_bytes": int64(42), "grpc.response.payload_bytes": int64(24)},
		FieldsProducer(ctx),
	)
	handler.HandleRPC(ctx, &stats.InPayload{Length: 38})
	require.Equal(t,
		logrus.Fields{"grpc.request.payload_bytes": int64(80), "grpc.response.payload_bytes": int64(24)},
		FieldsProducer(ctx),
	)
	handler.HandleRPC(ctx, &stats.OutPayload{Length: 66})
	require.Equal(t,
		logrus.Fields{"grpc.request.payload_bytes": int64(80), "grpc.response.payload_bytes": int64(90)},
		FieldsProducer(ctx),
	)
}

func TestPayloadBytesStats_Fields(t *testing.T) {
	bytesStats := PayloadBytesStats{InPayloadBytes: 80, OutPayloadBytes: 90}
	require.Equal(t, logrus.Fields{
		"grpc.request.payload_bytes":  int64(80),
		"grpc.response.payload_bytes": int64(90),
	}, bytesStats.Fields())
}

func TestFieldsProducer(t *testing.T) {
	ctx := testhelper.Context(t)

	t.Run("ok", func(t *testing.T) {
		handler := &PayloadBytes{}
		ctx := handler.TagRPC(ctx, nil)
		handler.HandleRPC(ctx, &stats.InPayload{Length: 42})
		handler.HandleRPC(ctx, &stats.OutPayload{Length: 24})
		require.Equal(t, logrus.Fields{
			"grpc.request.payload_bytes":  int64(42),
			"grpc.response.payload_bytes": int64(24),
		}, FieldsProducer(ctx))
	})

	t.Run("no data", func(t *testing.T) {
		require.Nil(t, FieldsProducer(ctx))
	})
}
