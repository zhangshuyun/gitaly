package featureflag

// LinkRepositoryToObjectPoolNotFound disables the implicit pool creation when linking a repository
// to an object pool that does not exist.
var LinkRepositoryToObjectPoolNotFound = NewFeatureFlag("link_repository_to_object_pool_not_found", false)
