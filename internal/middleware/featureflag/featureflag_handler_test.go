package featureflag

import (
	"context"
	"io/ioutil"
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"google.golang.org/grpc"
)

func TestUnaryInterceptor(t *testing.T) {
	t.Run("no feature flags", func(t *testing.T) {
		ctx, hook := setupContext()
		callUnary(ctx)
		require.Len(t, hook.AllEntries(), 1)
		require.Empty(t, hook.LastEntry().Data)
	})

	t.Run("multiple feature flags", func(t *testing.T) {
		ctx, hook := setup()
		callUnary(ctx)
		verify(t, hook)
	})
}

func TestStreamInterceptor(t *testing.T) {
	t.Run("no feature flags", func(t *testing.T) {
		ctx, hook := setupContext()
		callStream(ctx)
		require.Len(t, hook.AllEntries(), 1)
		require.Empty(t, hook.LastEntry().Data)
	})

	t.Run("multiple feature flags", func(t *testing.T) {
		ctx, hook := setup()
		callStream(ctx)
		verify(t, hook)
	})
}

func callUnary(ctx context.Context) {
	// nolint: errcheck
	UnaryInterceptor(ctx, nil, nil, func(context.Context, interface{}) (interface{}, error) {
		ctxlogrus.Extract(ctx).Info("verify")
		return nil, nil
	})
}

func callStream(ctx context.Context) {
	// nolint: errcheck
	StreamInterceptor(ctx, &grpc_middleware.WrappedServerStream{WrappedContext: ctx}, nil, func(interface{}, grpc.ServerStream) error {
		ctxlogrus.Extract(ctx).Info("verify")
		return nil
	})
}

func setup() (context.Context, *test.Hook) {
	ctx, hook := setupContext()
	ff1 := featureflag.FeatureFlag{Name: "ff1"}
	ff2 := featureflag.FeatureFlag{Name: "ff2"}
	ctx = featureflag.IncomingCtxWithDisabledFeatureFlag(ctx, ff1)
	ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, ff2)

	return ctx, hook
}

func setupContext() (context.Context, *test.Hook) {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetOutput(ioutil.Discard)

	hook := test.NewLocal(logger)
	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))
	return ctx, hook
}

func verify(t *testing.T, hook *test.Hook) {
	t.Helper()

	require.Len(t, hook.AllEntries(), 1)
	require.Equal(t, logrus.Fields{"feature_flags": "ff1:false ff2:true"}, hook.LastEntry().Data)
}
