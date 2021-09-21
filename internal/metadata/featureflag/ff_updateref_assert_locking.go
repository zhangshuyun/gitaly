package featureflag

// UpdaterefVerifyStateChanges causes us to assert that transactional state changes finish correctly.
var UpdaterefVerifyStateChanges = NewFeatureFlag("updateref_verify_state_changes", true)
