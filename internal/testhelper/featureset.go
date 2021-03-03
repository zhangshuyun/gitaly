package testhelper

import (
	"context"
	"sort"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
)

// FeatureSet is a representation of a set of features that should be disabled.
// This is useful in situations where a test needs to test any combination of features toggled on and off.
// It is designed to disable features as all features are enabled by default, please see: testhelper.Context()
type FeatureSet struct {
	features     map[featureflag.FeatureFlag]struct{}
	rubyFeatures map[featureflag.FeatureFlag]struct{}
}

// Desc describes the feature such that it is suitable as a testcase description.
func (f FeatureSet) Desc() string {
	features := make([]string, 0, len(f.features))

	for feature := range f.features {
		features = append(features, feature.Name)
	}
	for feature := range f.rubyFeatures {
		features = append(features, feature.Name)
	}

	if len(features) == 0 {
		return "all features enabled"
	}

	sort.Strings(features)

	return "disabled " + strings.Join(features, ",")
}

// Disable disables all feature flags in the given FeatureSet in the given context. The context is
// treated as an outgoing context.
func (f FeatureSet) Disable(ctx context.Context) context.Context {
	for feature := range f.features {
		ctx = featureflag.OutgoingCtxWithFeatureFlagValue(ctx, feature, "false")
		ctx = featureflag.IncomingCtxWithDisabledFeatureFlag(ctx, feature)
	}
	for feature := range f.rubyFeatures {
		ctx = featureflag.OutgoingCtxWithRubyFeatureFlagValue(ctx, feature, "false")
		ctx = featureflag.IncomingCtxWithRubyFeatureFlagValue(ctx, feature, false)
	}
	return ctx
}

// FeatureSets is a slice containing many FeatureSets
type FeatureSets []FeatureSet

// NewFeatureSets takes a slice of go feature flags, and an optional variadic set of ruby feature flags
// and returns a FeatureSets slice
func NewFeatureSets(goFeatures []featureflag.FeatureFlag, rubyFeatures ...featureflag.FeatureFlag) FeatureSets {
	var sets FeatureSets

	length := len(goFeatures) + len(rubyFeatures)

	// We want to generate all combinations of Go and Ruby features, which is 2^len(flags). To
	// do so, we simply iterate through all numbers from [0,len(flags)-1]. For each iteration, a
	// feature flag is added if its corresponding bit at the current iteration counter is 1,
	// otherwise it's left out of the set. Note that this also includes the empty set.
	for i := uint(0); i < uint(1<<length); i++ {
		set := FeatureSet{
			features:     make(map[featureflag.FeatureFlag]struct{}),
			rubyFeatures: make(map[featureflag.FeatureFlag]struct{}),
		}

		for j, feature := range goFeatures {
			if (i>>uint(j))&1 == 1 {
				set.features[feature] = struct{}{}
			}
		}

		for j, feature := range rubyFeatures {
			if (i>>uint(j+len(goFeatures)))&1 == 1 {
				set.rubyFeatures[feature] = struct{}{}
			}
		}

		sets = append(sets, set)
	}

	return sets
}

// Run executes the given test function for each of the FeatureSets. The passed in context has the
// feature flags set accordingly.
func (s FeatureSets) Run(t *testing.T, test func(t *testing.T, ctx context.Context)) {
	t.Helper()

	for _, featureSet := range s {
		t.Run(featureSet.Desc(), func(t *testing.T) {
			ctx, cancel := Context()
			defer cancel()
			ctx = featureSet.Disable(ctx)

			test(t, ctx)
		})
	}
}
