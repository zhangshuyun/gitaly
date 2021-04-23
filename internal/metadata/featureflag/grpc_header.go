package featureflag

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/metadata"
)

var (
	// featureFlagsOverride allows to enable all feature flags with a
	// single environment variable. If the value of
	// GITALY_TESTING_ENABLE_ALL_FEATURE_FLAGS is set to "true", then all
	// feature flags will be enabled. This is only used for testing
	// purposes such that we can run integration tests with feature flags.
	featureFlagsOverride = os.Getenv("GITALY_TESTING_ENABLE_ALL_FEATURE_FLAGS")

	flagChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_feature_flag_checks_total",
			Help: "Number of enabled/disabled checks for Gitaly server side feature flags",
		},
		[]string{"flag", "enabled"},
	)
)

// Delim is a delimiter used between a feature flag name and its value.
const Delim = ":"

// IsEnabled checks if the feature flag is enabled for the passed context.
// Only returns true if the metadata for the feature flag is set to "true"
func IsEnabled(ctx context.Context, flag FeatureFlag) bool {
	if featureFlagsOverride == "true" {
		return true
	}

	val, ok := getFlagVal(ctx, flag.Name)
	if !ok {
		return flag.OnByDefault
	}

	enabled := val == "true"

	flagChecks.WithLabelValues(flag.Name, strconv.FormatBool(enabled)).Inc()

	return enabled
}

func getFlagVal(ctx context.Context, flag string) (string, bool) {
	if flag == "" {
		return "", false
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	val, ok := md[HeaderKey(flag)]
	if !ok {
		return "", false
	}

	if len(val) == 0 {
		return "", false
	}

	return val[0], true
}

// IsDisabled is the inverse of IsEnabled
func IsDisabled(ctx context.Context, flag FeatureFlag) bool {
	return !IsEnabled(ctx, flag)
}

const ffPrefix = "gitaly-feature-"

// HeaderKey returns the feature flag key to be used in the metadata map
func HeaderKey(flag string) string {
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
