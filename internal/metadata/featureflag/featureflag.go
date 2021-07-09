package featureflag

import (
	"context"
	"strconv"

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
)

// FeatureFlag gates the implementation of new or changed functionality.
type FeatureFlag struct {
	// Name is the name of the feature flag.
	Name string `json:"name"`
	// OnByDefault is the default value if the feature flag is not explicitly set in
	// the incoming context.
	OnByDefault bool `json:"on_by_default"`
}

// IsEnabled checks if the feature flag is enabled for the passed context.
// Only returns true if the metadata for the feature flag is set to "true"
func (ff FeatureFlag) IsEnabled(ctx context.Context) bool {
	if featureFlagsOverride {
		return true
	}

	val, ok := getFlagVal(ctx, ff.Name)
	if !ok {
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

func getFlagVal(ctx context.Context, flag string) (string, bool) {
	if flag == "" {
		return "", false
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	val, ok := md[headerKey(flag)]
	if !ok {
		return "", false
	}

	if len(val) == 0 {
		return "", false
	}

	return val[0], true
}
