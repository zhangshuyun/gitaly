package featureflag

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"google.golang.org/grpc/metadata"
)

// EnableFeatureFlag is used in tests to enablea a feature flag in the context metadata
func EnableFeatureFlag(ctx context.Context, flag string) context.Context {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{HeaderKey(flag): "true"})
	}
	md.Set(HeaderKey(flag), "true")
	md.Set(catfile.SessionIDField, "abc1234")

	return metadata.NewOutgoingContext(context.Background(), md)
}
