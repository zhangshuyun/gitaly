package featureflag

// UserMergeBranchAccessError changes error handling such that errors returned by Rails'
// access checks are returned via error details instead of via the PreReceiveError field.
var UserMergeBranchAccessError = NewFeatureFlag("user_merge_branch_access_error", false)
