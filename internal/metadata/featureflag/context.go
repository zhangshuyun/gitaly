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
		md.Set(headerKey(flag.Name), "true")
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
		md.Set(headerKey(flag.Name), "false")
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

	md.Set(headerKey(flag.Name), val)

	return metadata.NewOutgoingContext(ctx, md)
}

// IncomingCtxWithFeatureFlag is used to enable a feature flag in the incoming
// context. This is NOT meant for use in clients that transfer the context
// across process boundaries.
func IncomingCtxWithFeatureFlag(ctx context.Context, flag FeatureFlag) context.Context {
	return incomingCtxWithFeatureFlagValue(ctx, headerKey(flag.Name), true)
}

// IncomingCtxWithDisabledFeatureFlag marks feature flag as disabled in the incoming context.
func IncomingCtxWithDisabledFeatureFlag(ctx context.Context, flag FeatureFlag) context.Context {
	return incomingCtxWithFeatureFlagValue(ctx, headerKey(flag.Name), false)
}

// IncomingCtxWithRubyFeatureFlagValue sets the feature flags status in the context.
func IncomingCtxWithRubyFeatureFlagValue(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return incomingCtxWithFeatureFlagValue(ctx, rubyHeaderKey(flag.Name), enabled)
}

func incomingCtxWithFeatureFlagValue(ctx context.Context, key string, enabled bool) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	value := "false"
	if enabled {
		value = "true"
	}

	md.Set(key, value)
	return metadata.NewIncomingContext(ctx, md)
}

// OutgoingCtxWithRubyFeatureFlags returns a new context populated with outgoing metadata that
// has the given set of Ruby feature flags enabled.
func OutgoingCtxWithRubyFeatureFlags(ctx context.Context, flags ...FeatureFlag) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	for _, flag := range flags {
		md.Set(rubyHeaderKey(flag.Name), "true")
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// OutgoingCtxWithRubyFeatureFlagValue returns context populated with outgoing metadata
// that contains ruby feature flags passed in.
func OutgoingCtxWithRubyFeatureFlagValue(ctx context.Context, flag FeatureFlag, val string) context.Context {
	if val != "true" && val != "false" {
		return ctx
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	md.Set(rubyHeaderKey(flag.Name), val)

	return metadata.NewOutgoingContext(ctx, md)
}

func rubyHeaderKey(flag string) string {
	return fmt.Sprintf("gitaly-feature-ruby-%s", strings.ReplaceAll(flag, "_", "-"))
}
