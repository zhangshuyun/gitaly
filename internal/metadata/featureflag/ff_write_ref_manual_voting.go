package featureflag

// TransactionalSymbolicRefUpdates allows the WriteRef RPC to use an implementation to update HEAD that does
// its own transaction voting.
var TransactionalSymbolicRefUpdates = NewFeatureFlag("transactional_symbolic_ref_updates", false)
