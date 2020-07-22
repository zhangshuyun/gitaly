package featureflag

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// OutgoingCtxWithFeatureFlags is used to enable feature flags in the outgoing
// context metadata. The returned context is meant to be used in a client where
// the outcoming context is transferred to an incoming context.
func OutgoingCtxWithFeatureFlags(ctx context.Context, flags ...FeatureFlag) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	for _, flag := range flags {
		md.Set(HeaderKey(flag.GetName()), "true")
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// OutgoingCtxWithDisabledFeatureFlags is used to explicitly disable "on by
// default" feature flags in the outgoing context metadata. The returned context
// is meant to be used in a client where the outcoming context is transferred to
// an incoming context.
func OutgoingCtxWithDisabledFeatureFlags(ctx context.Context, flags ...FeatureFlag) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	for _, flag := range flags {
		md.Set(HeaderKey(flag.GetName()), "false")
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// OutgoingCtxWithFeatureFlagValue is used to set feature flags with an explicit value.
// only "true" or "false" are valid values. Any other value will be ignored.
func OutgoingCtxWithFeatureFlagValue(ctx context.Context, flag FeatureFlag, val string) context.Context {
	if val != "true" && val != "false" {
		return ctx
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	md.Set(HeaderKey(flag.GetName()), val)

	return metadata.NewOutgoingContext(ctx, md)
}

// IncomingCtxWithFeatureFlag is used to enable a feature flag in the incoming
// context. This is NOT meant for use in clients that transfer the context
// across process boundaries.
func IncomingCtxWithFeatureFlag(ctx context.Context, flag FeatureFlag) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}
	md.Set(HeaderKey(flag.GetName()), "true")
	return metadata.NewIncomingContext(ctx, md)
}

func OutgoingCtxWithRubyFeatureFlags(ctx context.Context, flags ...FeatureFlag) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	for _, flag := range flags {
		md.Set(rubyHeaderKey(flag.GetName()), "true")
	}

	return metadata.NewOutgoingContext(ctx, md)
}

func rubyHeaderKey(flag string) string {
	return fmt.Sprintf("gitaly-feature-ruby-%s", strings.ReplaceAll(flag, "_", "-"))
}
