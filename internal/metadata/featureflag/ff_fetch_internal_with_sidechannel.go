package featureflag

// FetchInternalWithSidechannel enables the use of SSHUploadPackWithSidechannel for internal
// fetches.
var FetchInternalWithSidechannel = NewFeatureFlag("fetch_internal_with_sidechannel", false)
