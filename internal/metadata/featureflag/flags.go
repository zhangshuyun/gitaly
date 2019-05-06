package featureflag

const (
	// CatfileCacheKey is the feature flag key for catfile batch caching. This should match
	// what is in gitlab-ce
	CatfileCacheKey = "catfile-cache"

	// DeltaIslandsKey controls whether we use Git delta islands during a repack.
	DeltaIslandsKey = "delta-islands"
)
