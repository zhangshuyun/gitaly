package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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

			assert.Equal(t, tc.enabled, IsEnabled(ctx, GoFeatureFlag{tc.flag, tc.onByDefault}))
		})
	}
}

func TestAllEnabledFlags(t *testing.T) {
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.New(
			map[string]string{
				ffPrefix + "meow": "true",
				ffPrefix + "foo":  "true",
				ffPrefix + "woof": "false", // not enabled
				ffPrefix + "bar":  "TRUE",  // not enabled
			},
		),
	)
	assert.ElementsMatch(t, AllFlags(ctx), []string{"meow:true", "foo:true", "woof:false", "bar:TRUE"})
}
