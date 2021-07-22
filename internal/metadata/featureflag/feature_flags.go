package featureflag

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// GoSetConfig enables git2go implementation of SetConfig.
	GoSetConfig = FeatureFlag{Name: "go_set_config", OnByDefault: false}
	// CreateRepositoryFromBundleAtomicFetch will add the `--atomic` flag to git-fetch(1) in
	// order to reduce the number of transactional votes.
	CreateRepositoryFromBundleAtomicFetch = FeatureFlag{Name: "create_repository_from_bundle_atomic_fetch", OnByDefault: true}
	// ResolveConflictsWithHooks will cause the ResolveConflicts RPC to run Git hooks after committing changes
	// to the branch.
	ResolveConflictsWithHooks = FeatureFlag{Name: "resolve_conflicts_with_hooks", OnByDefault: true}
	// ReplicateRepositoryDirectFetch will cause the ReplicateRepository RPC to perform fetches
	// via a direct call instead of doing an RPC call to its own server. This fixes calls of
	// `ReplicateRepository()` in case it's invoked via Praefect with transactions enabled.
	ReplicateRepositoryDirectFetch = FeatureFlag{Name: "replicate_repository_direct_fetch", OnByDefault: false}
	// FindAllTagsPipeline enables the alternative pipeline implementation for finding
	// tags via FindAllTags.
	FindAllTagsPipeline = FeatureFlag{Name: "find_all_tags_pipeline", OnByDefault: false}
	// TxRemoveRepository enables transactionsal voting for the RemoveRepository RPC.
	TxRemoveRepository = FeatureFlag{Name: "tx_remove_repository", OnByDefault: false}
	// QuarantinedUserCreateTag enables use of a quarantine object directory for UserCreateTag.
	QuarantinedUserCreateTag = FeatureFlag{Name: "quarantined_user_create_tag", OnByDefault: false}
	// UserSquashWithoutWorktree enables the new implementation of UserSquash which does not
	// require a worktree to compute the squash.
	UserSquashWithoutWorktree = FeatureFlag{Name: "user_squash_without_worktree", OnByDefault: false}
	// QuarantinedResolveCOnflicts enables use of a quarantine object directory for ResolveConflicts.
	QuarantinedResolveConflicts = FeatureFlag{Name: "quarantined_resolve_conflicts", OnByDefault: false}
	// GoUserApplyPatch enables the Go implementation of UserApplyPatch
	GoUserApplyPatch = FeatureFlag{Name: "go_user_apply_patch", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoSetConfig,
	CreateRepositoryFromBundleAtomicFetch,
	ResolveConflictsWithHooks,
	ReplicateRepositoryDirectFetch,
	FindAllTagsPipeline,
	TxRemoveRepository,
	QuarantinedUserCreateTag,
	UserSquashWithoutWorktree,
	QuarantinedResolveConflicts,
	GoUserApplyPatch,
}
