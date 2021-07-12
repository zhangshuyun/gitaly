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

// headerKey returns the feature flag key to be used in the metadata map
func headerKey(flag string) string {
	return ffPrefix + strings.ReplaceAll(flag, "_", "-")
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
