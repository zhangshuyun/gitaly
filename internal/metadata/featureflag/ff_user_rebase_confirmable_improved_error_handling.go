package featureflag

// UserRebaseConfirmableImprovedErrorHandling enables proper error handling in the UserRebaseConfirmable RPC. When this
// flag is disabled many error cases were returning successfully with an error message embedded in
// the response. With this flag enabled, this is converted to return real gRPC errors with
// structured errors.
var UserRebaseConfirmableImprovedErrorHandling = NewFeatureFlag("user_rebase_confirmable_improved_error_handling", false)
