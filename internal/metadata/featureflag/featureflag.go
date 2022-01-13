package featureflag

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"google.golang.org/grpc/metadata"
)

var (
	// featureFlagsOverride allows to enable all feature flags with a
	// single environment variable. If the value of
	// GITALY_TESTING_ENABLE_ALL_FEATURE_FLAGS is set to "true", then all
	// feature flags will be enabled. This is only used for testing
	// purposes such that we can run integration tests with feature flags.
	featureFlagsOverride, _ = env.GetBool("GITALY_TESTING_ENABLE_ALL_FEATURE_FLAGS", false)

	flagChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_feature_flag_checks_total",
			Help: "Number of enabled/disabled checks for Gitaly server side feature flags",
		},
		[]string{"flag", "enabled"},
	)

	// All is the list of all registered feature flags.
	All = []FeatureFlag{}
)

const explicitFeatureFlagKey = "require_explicit_feature_flag_checks"

func injectIntoIncomingAndOutgoingContext(ctx context.Context, key string, enabled bool) context.Context {
	incomingMD, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		incomingMD = metadata.New(map[string]string{})
	}

	incomingMD.Set(key, strconv.FormatBool(enabled))

	ctx = metadata.NewIncomingContext(ctx, incomingMD)

	return metadata.AppendToOutgoingContext(ctx, key, strconv.FormatBool(enabled))
}

// ContextWithExplicitFeatureFlags marks the context such that all feature flags which are checked
// must have been explicitly set in that context. If a feature flag wasn't set to an explicit value,
// then checking this feature flag will panic. This is not for use in production systems, but is
// intended for tests to verify that we test each feature flag properly.
func ContextWithExplicitFeatureFlags(ctx context.Context) context.Context {
	return injectIntoIncomingAndOutgoingContext(ctx, explicitFeatureFlagKey, true)
}

// FeatureFlag gates the implementation of new or changed functionality.
type FeatureFlag struct {
	// Name is the name of the feature flag.
	Name string `json:"name"`
	// OnByDefault is the default value if the feature flag is not explicitly set in
	// the incoming context.
	OnByDefault bool `json:"on_by_default"`
}

// NewFeatureFlag creates a new feature flag and adds it to the array of all existing feature flags.
func NewFeatureFlag(name string, onByDefault bool) FeatureFlag {
	featureFlag := FeatureFlag{
		Name:        name,
		OnByDefault: onByDefault,
	}
	All = append(All, featureFlag)
	return featureFlag
}

// IsEnabled checks if the feature flag is enabled for the passed context.
// Only returns true if the metadata for the feature flag is set to "true"
func (ff FeatureFlag) IsEnabled(ctx context.Context) bool {
	if featureFlagsOverride {
		return true
	}

	val, ok := ff.valueFromContext(ctx)
	if !ok {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if _, ok := md[explicitFeatureFlagKey]; ok {
				panic(fmt.Sprintf("checking for feature %q without use of feature sets", ff.Name))
			}
		}

		return ff.OnByDefault
	}

	enabled := val == "true"

	flagChecks.WithLabelValues(ff.Name, strconv.FormatBool(enabled)).Inc()

	return enabled
}

// IsDisabled determines whether the feature flag is disabled in the incoming context.
func (ff FeatureFlag) IsDisabled(ctx context.Context) bool {
	return !ff.IsEnabled(ctx)
}

// MetadataKey returns the key of the feature flag as it is present in the metadata map.
func (ff FeatureFlag) MetadataKey() string {
	return ffPrefix + strings.ReplaceAll(ff.Name, "_", "-")
}

func (ff FeatureFlag) valueFromContext(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	val, ok := md[ff.MetadataKey()]
	if !ok {
		return "", false
	}

	if len(val) == 0 {
		return "", false
	}

	return val[0], true
}
