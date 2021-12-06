package featureflag

// RenameRepositoryLocking enables locking semantics for the RenameRepository RPC.
var RenameRepositoryLocking = NewFeatureFlag("rename_repository_locking", false)
