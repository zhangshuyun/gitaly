package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	ff "gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
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
			features: []ff.FeatureFlag{ff.GoFetchSourceBranch},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(ff.GoFetchSourceBranch),
					rubyFeatures: features(),
				},
			},
		},
		{
			desc:     "two Go feature flags",
			features: []ff.FeatureFlag{ff.GoFetchSourceBranch, ff.DistributedReads},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(ff.DistributedReads),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(ff.GoFetchSourceBranch),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(ff.DistributedReads, ff.GoFetchSourceBranch),
					rubyFeatures: features(),
				},
			},
		},
		{
			desc:         "single Ruby feature flag",
			rubyFeatures: []ff.FeatureFlag{ff.GoFetchSourceBranch},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(ff.GoFetchSourceBranch),
				},
			},
		},
		{
			desc:         "two Ruby feature flags",
			rubyFeatures: []ff.FeatureFlag{ff.GoFetchSourceBranch, ff.DistributedReads},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(ff.DistributedReads),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(ff.GoFetchSourceBranch),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(ff.DistributedReads, ff.GoFetchSourceBranch),
				},
			},
		},
		{
			desc:         "Go and Ruby feature flag",
			features:     []ff.FeatureFlag{ff.DistributedReads},
			rubyFeatures: []ff.FeatureFlag{ff.GoFetchSourceBranch},
			expected: FeatureSets{
				FeatureSet{
					features:     features(),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(ff.DistributedReads),
					rubyFeatures: features(),
				},
				FeatureSet{
					features:     features(),
					rubyFeatures: features(ff.GoFetchSourceBranch),
				},
				FeatureSet{
					features:     features(ff.DistributedReads),
					rubyFeatures: features(ff.GoFetchSourceBranch),
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
	var flags [][2]bool

	NewFeatureSets([]ff.FeatureFlag{
		ff.DistributedReads, ff.GoFetchSourceBranch,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		ctx = helper.OutgoingToIncoming(ctx)
		flags = append(flags, [2]bool{
			ff.IsDisabled(ctx, ff.DistributedReads),
			ff.IsDisabled(ctx, ff.GoFetchSourceBranch),
		})
	})

	require.Equal(t, flags, [][2]bool{
		{false, false},
		{true, false},
		{false, true},
		{true, true},
	})
}
