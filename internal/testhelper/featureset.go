package testhelper

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
)

// FeatureSet is a representation of a set of features that should be disabled.
// This is useful in situations where a test needs to test any combination of features toggled on and off.
// It is designed to disable features as all features are enabled by default, please see: testhelper.Context()
type FeatureSet struct {
	features     map[featureflag.FeatureFlag]bool
	rubyFeatures map[featureflag.FeatureFlag]bool
}

// Desc describes the feature such that it is suitable as a testcase description.
func (f FeatureSet) Desc() string {
	features := make([]string, 0, len(f.features)+len(f.rubyFeatures))

	for feature, enabled := range f.features {
		features = append(features, fmt.Sprintf("%s=%s", feature.Name, strconv.FormatBool(enabled)))
	}
	for feature, enabled := range f.rubyFeatures {
		features = append(features, fmt.Sprintf("%s=%s", feature.Name, strconv.FormatBool(enabled)))
	}

	sort.Strings(features)

	return strings.Join(features, ",")
}

// Apply applies all feature flags in the given FeatureSet to the given context.
func (f FeatureSet) Apply(ctx context.Context) context.Context {
	for feature, enabled := range f.features {
		ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, feature, enabled)
		ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, feature, enabled)
	}
	for feature, enabled := range f.rubyFeatures {
		ctx = featureflag.OutgoingCtxWithRubyFeatureFlag(ctx, feature, enabled)
		ctx = featureflag.IncomingCtxWithRubyFeatureFlag(ctx, feature, enabled)
	}
	return ctx
}

// FeatureSets is a slice containing many FeatureSets
type FeatureSets []FeatureSet

// NewFeatureSets takes Go feature flags and returns the combination of FeatureSets.
func NewFeatureSets(features ...featureflag.FeatureFlag) FeatureSets {
	return NewFeatureSetsWithRubyFlags(features, nil)
}

// NewFeatureSetsWithRubyFlags takes a Go- and Ruby-specific feature flags and returns a the
// combination of FeatureSets.
func NewFeatureSetsWithRubyFlags(goFeatures []featureflag.FeatureFlag, rubyFeatures []featureflag.FeatureFlag) FeatureSets {
	var sets FeatureSets

	length := len(goFeatures) + len(rubyFeatures)

	// We want to generate all combinations of Go and Ruby features, which is 2^len(flags). To
	// do so, we simply iterate through all numbers from [0,len(flags)-1]. For each iteration, a
	// feature flag is added if its corresponding bit at the current iteration counter is 1,
	// otherwise it's left out of the set. Note that this also includes the empty set.
	for i := uint(0); i < uint(1<<length); i++ {
		set := FeatureSet{
			features:     make(map[featureflag.FeatureFlag]bool),
			rubyFeatures: make(map[featureflag.FeatureFlag]bool),
		}

		for j, feature := range goFeatures {
			set.features[feature] = (i>>uint(j))&1 == 1
		}
		for j, feature := range rubyFeatures {
			set.rubyFeatures[feature] = (i>>uint(j+len(goFeatures)))&1 == 1
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
			ctx = featureSet.Apply(ctx)

			test(t, ctx)
		})
	}
}

// Bench executes the given benchmarking function for each of the FeatureSets. The passed in
// context has the feature flags set accordingly.
func (s FeatureSets) Bench(b *testing.B, test func(b *testing.B, ctx context.Context)) {
	b.Helper()

	for _, featureSet := range s {
		b.Run(featureSet.Desc(), func(b *testing.B) {
			ctx, cancel := Context()
			defer cancel()
			ctx = featureSet.Apply(ctx)

			test(b, ctx)
		})
	}
}
