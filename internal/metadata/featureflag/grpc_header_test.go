package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestGRPCMetadataFeatureFlag(t *testing.T) {
	testCases := []struct {
		flag        string
		headers     map[string]string
		enabled     bool
		onByDefault bool
		desc        string
	}{
		{"", nil, false, false, "empty name and no headers"},
		{"flag", nil, false, false, "no headers"},
		{"flag", map[string]string{"flag": "true"}, false, false, "no 'gitaly-feature' prefix in flag name"},
		{"flag", map[string]string{"gitaly-feature-flag": "TRUE"}, false, false, "not valid header value"},
		{"flag_under_score", map[string]string{"gitaly-feature-flag-under-score": "true"}, true, false, "flag name with underscores"},
		{"flag-dash-ok", map[string]string{"gitaly-feature-flag-dash-ok": "true"}, true, false, "flag name with dashes"},
		{"flag", map[string]string{"gitaly-feature-flag": "false"}, false, true, "flag explicitly disabled"},
		{"flag", map[string]string{}, true, true, "flag enabled by default but missing"},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			md := metadata.New(tc.headers)
			ctx := metadata.NewIncomingContext(context.Background(), md)

			require.Equal(t, tc.enabled, IsEnabled(ctx, FeatureFlag{tc.flag, tc.onByDefault}))
		})
	}
}

func TestAllEnabledFlags(t *testing.T) {
	flags := map[string]string{
		ffPrefix + "meow": "true",
		ffPrefix + "foo":  "true",
		ffPrefix + "woof": "false", // not enabled
		ffPrefix + "bar":  "TRUE",  // not enabled
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(flags))
	require.ElementsMatch(t, AllFlags(ctx), []string{"meow:true", "foo:true", "woof:false", "bar:TRUE"})
}

func TestRaw(t *testing.T) {
	enabledFlag := FeatureFlag{Name: "enabled-flag"}
	disabledFlag := FeatureFlag{Name: "disabled-flag"}

	raw := Raw{
		ffPrefix + enabledFlag.Name:  "true",
		ffPrefix + disabledFlag.Name: "false",
	}

	t.Run("RawFromContext", func(t *testing.T) {
		ctx := context.Background()
		ctx = IncomingCtxWithFeatureFlag(ctx, enabledFlag)
		ctx = IncomingCtxWithDisabledFeatureFlag(ctx, disabledFlag)

		require.Equal(t, raw, RawFromContext(ctx))
	})

	t.Run("OutgoingWithRaw", func(t *testing.T) {
		outgoingMD, ok := metadata.FromOutgoingContext(OutgoingWithRaw(context.Background(), raw))
		require.True(t, ok)
		require.Equal(t, metadata.MD{
			ffPrefix + enabledFlag.Name:  {"true"},
			ffPrefix + disabledFlag.Name: {"false"},
		}, outgoingMD)
	})
}
