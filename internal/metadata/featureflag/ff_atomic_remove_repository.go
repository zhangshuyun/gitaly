package featureflag

// AtomicRemoveRepository enables locking semantics for RemoveRepository.
var AtomicRemoveRepository = NewFeatureFlag("atomic_remove_repository", false)
