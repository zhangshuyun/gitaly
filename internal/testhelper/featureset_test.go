package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	ff "gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"google.golang.org/grpc/metadata"
)

var (
	featureFlagA = ff.FeatureFlag{Name: "test_feature_flag_a"}
	featureFlagB = ff.FeatureFlag{Name: "test_feature_flag_b"}
)

func features(flag ...ff.FeatureFlag) map[ff.FeatureFlag]struct{} {
	features := make(map[ff.FeatureFlag]struct{}, len(flag))
	for _, f := range flag {
		features[f] = struct{}{}
	}
	return features
}

func TestNewFeatureSets(t *testing.T) {
	testcases := []struct {
		desc         string
		features     []ff.FeatureFlag
		rubyFeatures []ff.FeatureFlag
		expected     FeatureSets
	}{
		{
			desc:     "single Go feature flag",
			features: []ff.FeatureFlag{featureFlagA},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(featureFlagA),
					rubyFeatures: features(),
				},
			},
		},
		{
			desc:     "two Go feature flags",
			features: []ff.FeatureFlag{featureFlagA, featureFlagB},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(featureFlagB),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(featureFlagA),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(featureFlagB, featureFlagA),
					rubyFeatures: features(),
				},
			},
		},
		{
			desc:         "single Ruby feature flag",
			rubyFeatures: []ff.FeatureFlag{featureFlagA},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(featureFlagA),
				},
			},
		},
		{
			desc:         "two Ruby feature flags",
			rubyFeatures: []ff.FeatureFlag{featureFlagA, featureFlagB},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(featureFlagB),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(featureFlagA),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(featureFlagB, featureFlagA),
				},
			},
		},
		{
			desc:         "Go and Ruby feature flag",
			features:     []ff.FeatureFlag{featureFlagB},
			rubyFeatures: []ff.FeatureFlag{featureFlagA},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(featureFlagB),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(featureFlagA),
				},
				FeatureSet{
					features:     features(featureFlagB),
					rubyFeatures: features(featureFlagA),
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			featureSets := NewFeatureSets(tc.features, tc.rubyFeatures...)
			require.Len(t, featureSets, len(tc.expected))
			for _, expected := range tc.expected {
				require.Contains(t, featureSets, expected)
			}
		})
	}
}

func TestFeatureSets_Run(t *testing.T) {
	var incomingFlags [][2]bool
	var outgoingFlags [][2]bool

	// This test depends on feature flags being default-enabled in the test
	// context, which requires those flags to exist in the ff.All slice. So
	// let's just append them here so we do not need to use a "real"
	// feature flag, as that would require constant change when we remove
	// old feature flags.
	defer func(old []ff.FeatureFlag) {
		ff.All = old
	}(ff.All)
	ff.All = append(ff.All, featureFlagA, featureFlagB)

	NewFeatureSets([]ff.FeatureFlag{
		featureFlagB, featureFlagA,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		incomingMD, ok := metadata.FromIncomingContext(ctx)
		require.True(t, ok)

		outgoingMD, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)

		incomingCtx := metadata.NewIncomingContext(context.Background(), incomingMD)
		outgoingCtx := helper.OutgoingToIncoming(metadata.NewOutgoingContext(context.Background(), outgoingMD))

		incomingFlags = append(incomingFlags, [2]bool{
			ff.IsDisabled(incomingCtx, featureFlagB),
			ff.IsDisabled(incomingCtx, featureFlagA),
		})
		outgoingFlags = append(outgoingFlags, [2]bool{
			ff.IsDisabled(outgoingCtx, featureFlagB),
			ff.IsDisabled(outgoingCtx, featureFlagA),
		})
	})

	for _, tc := range []struct {
		desc  string
		flags [][2]bool
	}{
		{desc: "incoming context", flags: incomingFlags},
		{desc: "outgoing context", flags: outgoingFlags},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.ElementsMatch(t, tc.flags, [][2]bool{
				{false, false},
				{true, false},
				{false, true},
				{true, true},
			})
		})
	}
}
