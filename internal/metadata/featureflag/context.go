package featureflag

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	// Delim is a delimiter used between a feature flag name and its value.
	Delim = ":"

	// ffPrefix is the prefix used for Gitaly-scoped feature flags.
	ffPrefix = "gitaly-feature-"
)

// ContextWithFeatureFlag sets the feature flag in both the incoming and outgoing context.
func ContextWithFeatureFlag(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return injectIntoIncomingAndOutgoingContext(ctx, flag.MetadataKey(), enabled)
}

// OutgoingCtxWithFeatureFlag sets the feature flag for an outgoing context.
func OutgoingCtxWithFeatureFlag(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return outgoingCtxWithFeatureFlag(ctx, flag.MetadataKey(), enabled)
}

// OutgoingCtxWithRubyFeatureFlag sets the Ruby feature flag for an outgoing context.
func OutgoingCtxWithRubyFeatureFlag(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return outgoingCtxWithFeatureFlag(ctx, rubyHeaderKey(flag.Name), enabled)
}

func outgoingCtxWithFeatureFlag(ctx context.Context, key string, enabled bool) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	md = md.Copy()
	md.Set(key, strconv.FormatBool(enabled))

	return metadata.NewOutgoingContext(ctx, md)
}

// IncomingCtxWithFeatureFlag sets the feature flag for an incoming context. This is NOT meant for
// use in clients that transfer the context across process boundaries.
func IncomingCtxWithFeatureFlag(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return incomingCtxWithFeatureFlag(ctx, flag.MetadataKey(), enabled)
}

// IncomingCtxWithRubyFeatureFlag sets the Ruby feature flag for an incoming context. This is NOT
// meant for use in clients that transfer the context across process boundaries.
func IncomingCtxWithRubyFeatureFlag(ctx context.Context, flag FeatureFlag, enabled bool) context.Context {
	return incomingCtxWithFeatureFlag(ctx, rubyHeaderKey(flag.Name), enabled)
}

func incomingCtxWithFeatureFlag(ctx context.Context, key string, enabled bool) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(map[string]string{})
	}

	md = md.Copy()
	md.Set(key, strconv.FormatBool(enabled))

	return metadata.NewIncomingContext(ctx, md)
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
