package featureflag

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

// IsEnabled checks if the feature flag is enabled for the passed context.
// Only return true if the metadata for the feature flag is set to "true"
func IsEnabled(ctx context.Context, flag string) bool {
	if flag == "" {
		return false
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	val, ok := md[HeaderKey(flag)]
	if !ok {
		return false
	}

	return len(val) > 0 && val[0] == "true"
}

// IsDisabled is the inverse operation of IsEnabled
func IsDisabled(ctx context.Context, flag string) bool {
	return !IsEnabled(ctx, flag)
}

// HeaderKey returns the feature flag key to be used in the metadata map
func HeaderKey(flag string) string {
	return fmt.Sprintf("gitaly-feature-%s", flag)
}
