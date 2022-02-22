package featureflag

// GitV2351WithFetchSpeedups will enable the use of Git v2.35.1 with patches speeding up mirror
// fetches in repositories with many references.
var GitV2351WithFetchSpeedups = NewFeatureFlag("git_v2351_with_fetch_speedups", false)
