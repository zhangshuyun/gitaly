package featureflag

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	// Delim is a delimiter used between a feature flag name and its value.
	Delim = ":"

	// ffPrefix is the prefix used for Gitaly-scoped feature flags.
	ffPrefix = "gitaly-feature-"
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
		md.Set(flag.MetadataKey(), "true")
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
		md.Set(flag.MetadataKey(), "false")
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

	md.Set(flag.MetadataKey(), val)

	return metadata.NewOutgoingContext(ctx, md)
}

// IncomingCtxWithFeatureFlag is used to enable a feature flag in the incoming
// context. This is NOT meant for use in clients that transfer the context
// across process boundaries.
func IncomingCtxWithFeatureFlag(ctx context.Context, flag FeatureFlag) context.Context {
	return incomingCtxWithFeatureFlagValue(ctx, flag.MetadataKey(), true)
}

// IncomingCtxWithDisabledFeatureFlag marks feature flag as disabled in the incoming context.
func IncomingCtxWithDisabledFeatureFlag(ctx context.Context, flag FeatureFlag) context.Context {
	return incomingCtxWithFeatureFlagValue(ctx, flag.MetadataKey(), false)
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

// AllFlags returns all feature flags with their value that use the Gitaly metadata
// prefix. Note: results will not be sorted.
func AllFlags(ctx context.Context) []string {
	featureFlagMap := RawFromContext(ctx)
	featureFlagSlice := make([]string, 0, len(featureFlagMap))
	for key, value := range featureFlagMap {
		featureFlagSlice = append(featureFlagSlice,
			fmt.Sprintf("%s%s%s", strings.TrimPrefix(key, ffPrefix), Delim, value),
		)
	}

	return featureFlagSlice
}

// Raw contains feature flags and their values in their raw form with header prefix in place
// and values unparsed.
type Raw map[string]string

// RawFromContext returns a map that contains all feature flags with their values. The feature
// flags are in their raw format with the header prefix in place. If multiple values are set a
// flag, the first occurrence is used.
//
// This is mostly intended for propagating the feature flags by other means than the metadata,
// for example into the hooks through the environment.
func RawFromContext(ctx context.Context) Raw {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	featureFlags := map[string]string{}
	for key, values := range md {
		if !strings.HasPrefix(key, ffPrefix) || len(values) == 0 {
			continue
		}

		featureFlags[key] = values[0]
	}

	return featureFlags
}

// OutgoingWithRaw returns a new context with raw flags appended into the outgoing
// metadata.
func OutgoingWithRaw(ctx context.Context, flags Raw) context.Context {
	for key, value := range flags {
		ctx = metadata.AppendToOutgoingContext(ctx, key, value)
	}

	return ctx
}

func rubyHeaderKey(flag string) string {
	return fmt.Sprintf("gitaly-feature-ruby-%s", strings.ReplaceAll(flag, "_", "-"))
}
