package featureflag

// ExactPaginationTokenMatch enables exact matching for provided pagination tokens and
// returns an error if the match is not found.
var ExactPaginationTokenMatch = NewFeatureFlag("exact_pagination_token_match", false)
