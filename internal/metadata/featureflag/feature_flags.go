package featureflag

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// GoSetConfig enables git2go implementation of SetConfig.
	GoSetConfig = FeatureFlag{Name: "go_set_config", OnByDefault: false}
	// ResolveConflictsWithHooks will cause the ResolveConflicts RPC to run Git hooks after committing changes
	// to the branch.
	ResolveConflictsWithHooks = FeatureFlag{Name: "resolve_conflicts_with_hooks", OnByDefault: true}
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
	// FetchInternalNoAlternateRefs disables use of alternate refs in internal fetches.
	FetchInternalNoAlternateRefs = FeatureFlag{Name: "fetch_internal_no_alternate_refs", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoSetConfig,
	ResolveConflictsWithHooks,
	FindAllTagsPipeline,
	TxRemoveRepository,
	QuarantinedUserCreateTag,
	UserSquashWithoutWorktree,
	QuarantinedResolveConflicts,
	GoUserApplyPatch,
	FetchInternalNoAlternateRefs,
}
