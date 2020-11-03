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

	if len(features) == 0 {
		return "all features enabled"
	}

	sort.Strings(features)

	return "disabled " + strings.Join(features, ",")
}

func (f FeatureSet) Disable(ctx context.Context) context.Context {
	for feature := range f.features {
		if _, ok := f.rubyFeatures[feature]; ok {
			ctx = featureflag.OutgoingCtxWithRubyFeatureFlagValue(ctx, feature, "false")
			continue
		}
		ctx = featureflag.OutgoingCtxWithFeatureFlagValue(ctx, feature, "false")
	}

	return ctx
}

// FeatureSets is a slice containing many FeatureSets
type FeatureSets []FeatureSet

// NewFeatureSets takes a slice of go feature flags, and an optional variadic set of ruby feature flags
// and returns a FeatureSets slice
func NewFeatureSets(goFeatures []featureflag.FeatureFlag, rubyFeatures ...featureflag.FeatureFlag) FeatureSets {
	rubyFeatureMap := make(map[featureflag.FeatureFlag]struct{})
	for _, rubyFeature := range rubyFeatures {
		rubyFeatureMap[rubyFeature] = struct{}{}
	}

	// start with an empty feature set
	f := []FeatureSet{{features: make(map[featureflag.FeatureFlag]struct{}), rubyFeatures: rubyFeatureMap}}

	allFeatures := append(goFeatures, rubyFeatures...)

	for i := range allFeatures {
		featureMap := make(map[featureflag.FeatureFlag]struct{})
		for j := 0; j <= i; j++ {
			featureMap[allFeatures[j]] = struct{}{}
		}

		f = append(f, FeatureSet{features: featureMap, rubyFeatures: rubyFeatureMap})
	}

	return f
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
