package featureflag

import (
	"context"
	"sort"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"google.golang.org/grpc"
)

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	track(ctx)
	return handler(ctx, req)
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	track(stream.Context())
	return handler(srv, stream)
}

// track adds the list of the feature flags into the logging context.
// The list is sorted by feature flag name to produce consistent output.
func track(ctx context.Context) {
	flags := featureflag.AllFlags(ctx)
	if len(flags) != 0 {
		sort.Slice(flags, func(i, j int) bool {
			return flags[i][:strings.Index(flags[i], featureflag.Delim)] < flags[j][:strings.Index(flags[j], featureflag.Delim)]
		})
		ctxlogrus.AddFields(ctx, logrus.Fields{"feature_flags": strings.Join(flags, " ")})
	}
}
