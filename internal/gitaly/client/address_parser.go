package client

import (
	"fmt"
	"net/url"
	"strings"
)

// extractHostFromRemoteURL will convert Gitaly-style URL addresses of the form
// scheme://host:port to the "host:port" addresses used by `grpc.Dial`
func extractHostFromRemoteURL(rawAddress string) (hostAndPort string, err error) {
	u, err := url.Parse(rawAddress)
	if err != nil {
		return "", fmt.Errorf("failed to parse remote addresses: %w", err)
	}

	if u.Path != "" {
		return "", fmt.Errorf("remote addresses should not have a path: %q", u.Path)
	}

	if u.Host == "" {
		return "", fmt.Errorf("remote addresses should have a host")
	}

	return u.Host, nil
}

// extractPathFromSocketURL will convert Gitaly-style URL addresses of the form
// unix:/path/to/socket into file paths: `/path/to/socket`
const unixPrefix = "unix:"

func extractPathFromSocketURL(rawAddress string) (socketPath string, err error) {
	if !strings.HasPrefix(rawAddress, unixPrefix) {
		return "", fmt.Errorf("invalid socket address: %s", rawAddress)
	}

	return strings.TrimPrefix(rawAddress, unixPrefix), nil
}
