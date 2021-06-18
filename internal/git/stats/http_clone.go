package stats

import (
	"context"
	"fmt"
)

// HTTPClone hosts information about a typical HTTP-based clone.
type HTTPClone struct {
	// ReferenceDiscovery is the reference discovery performed as part of the clone.
	ReferenceDiscovery HTTPReferenceDiscovery
	// FetchPack is the response to a git-fetch-pack(1) request which computes and transmits the
	// packfile.
	FetchPack HTTPFetchPack
}

// PerformHTTPClone does a Git HTTP clone, discarding cloned data to /dev/null.
func PerformHTTPClone(ctx context.Context, url, user, password string, interactive bool) (HTTPClone, error) {
	printInteractive := func(format string, a ...interface{}) {
		if interactive {
			// Ignore any errors returned by this given that we only use it as a
			// debugging aid to write to stdout.
			fmt.Printf(format, a...)
		}
	}

	referenceDiscovery, err := performHTTPReferenceDiscovery(ctx, url, user, password, printInteractive)
	if err != nil {
		return HTTPClone{}, ctxErr(ctx, err)
	}

	fetchPack, err := performFetchPack(ctx, url, user, password,
		referenceDiscovery.Refs(), printInteractive)
	if err != nil {
		return HTTPClone{}, ctxErr(ctx, err)
	}

	return HTTPClone{
		ReferenceDiscovery: referenceDiscovery,
		FetchPack:          fetchPack,
	}, nil
}

func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
