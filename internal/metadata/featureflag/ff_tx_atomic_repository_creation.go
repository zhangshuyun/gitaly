package featureflag

// TxAtomicRepositoryCreation will switch CreateRepository, CreateRepositoryFromBundle,
// CreateRepositoryFromSnapshot and CreateRepositoryFromURL to have proper transactional guarantees.
// This changes behaviour such the target repository must not exist previous to the call, creation
// and seeding of the repository is done in a temporary staging area and then moved into place only
// if no other RPC call created it concurrently.
var TxAtomicRepositoryCreation = NewFeatureFlag("tx_atomic_repository_creation", false)
