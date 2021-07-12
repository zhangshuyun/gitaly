package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestFeatureFlag_enabled(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		flag        string
		headers     map[string]string
		enabled     bool
		onByDefault bool
	}{
		{
			desc:        "empty name and no headers",
			flag:        "",
			headers:     nil,
			enabled:     false,
			onByDefault: false,
		},
		{
			desc:        "no headers",
			flag:        "flag",
			headers:     nil,
			enabled:     false,
			onByDefault: false,
		},
		{
			desc:        "no 'gitaly-feature' prefix in flag name",
			flag:        "flag",
			headers:     map[string]string{"flag": "true"},
			enabled:     false,
			onByDefault: false,
		},
		{
			desc:        "not valid header value",
			flag:        "flag",
			headers:     map[string]string{"gitaly-feature-flag": "TRUE"},
			enabled:     false,
			onByDefault: false,
		},
		{
			desc:        "flag name with underscores",
			flag:        "flag_under_score",
			headers:     map[string]string{"gitaly-feature-flag-under-score": "true"},
			enabled:     true,
			onByDefault: false,
		},
		{
			desc:        "flag name with dashes",
			flag:        "flag-dash-ok",
			headers:     map[string]string{"gitaly-feature-flag-dash-ok": "true"},
			enabled:     true,
			onByDefault: false,
		},
		{
			desc:        "flag explicitly disabled",
			flag:        "flag",
			headers:     map[string]string{"gitaly-feature-flag": "false"},
			enabled:     false,
			onByDefault: true,
		},
		{
			desc:        "flag enabled by default but missing",
			flag:        "flag",
			headers:     map[string]string{},
			enabled:     true,
			onByDefault: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.New(tc.headers))

			ff := FeatureFlag{tc.flag, tc.onByDefault}
			require.Equal(t, tc.enabled, ff.IsEnabled(ctx))
			require.Equal(t, tc.enabled, !ff.IsDisabled(ctx))
		})
	}
}
