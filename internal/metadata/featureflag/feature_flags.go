package featureflag

type FeatureFlag struct {
	Name        string `json:"name"`
	OnByDefault bool   `json:"on_by_default"`
}

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// GoFetchSourceBranch enables a go implementation of FetchSourceBranch
	GoFetchSourceBranch = FeatureFlag{Name: "go_fetch_source_branch", OnByDefault: true}
	// DistributedReads allows praefect to redirect accessor operations to up-to-date secondaries
	DistributedReads = FeatureFlag{Name: "distributed_reads", OnByDefault: true}
	// ReferenceTransactions will handle Git reference updates via the transaction service for strong consistency
	ReferenceTransactions = FeatureFlag{Name: "reference_transactions", OnByDefault: true}
	// LogCommandStats will log additional rusage stats for commands
	LogCommandStats = FeatureFlag{Name: "log_command_stats", OnByDefault: false}
	// GoUserFFBranch enables the Go implementation of UserFFBranch
	GoUserFFBranch = FeatureFlag{Name: "go_user_ff_branch", OnByDefault: false}
	// GoUserCreateBranch enables the Go implementation of UserCreateBranch
	GoUserCreateBranch = FeatureFlag{Name: "go_user_create_branch", OnByDefault: true}
	// GoUserUpdateBranch enables the Go implementation of UserUpdateBranch
	GoUserUpdateBranch = FeatureFlag{Name: "go_user_update_branch", OnByDefault: false}
	// GoUserCommitFiles enables the Go implementation of UserCommitFiles
	GoUserCommitFiles = FeatureFlag{Name: "go_user_commit_files", OnByDefault: false}
	// GoResolveConflicts enables the Go implementation of ResolveConflicts
	GoResolveConflicts = FeatureFlag{Name: "go_resolve_conflicts", OnByDefault: false}
	// GoUserUpdateSubmodule enables the Go implementation of
	// UserUpdateSubmodules
	GoUserUpdateSubmodule = FeatureFlag{Name: "go_user_update_submodule", OnByDefault: false}
	// GoUserCreateTag enables the Go implementation of UserCreateTag
	GoUserCreateTag = FeatureFlag{Name: "go_user_create_tag", OnByDefault: true}
	// GoUserRevert enables the Go implementation of UserRevert
	GoUserRevert = FeatureFlag{Name: "go_user_revert", OnByDefault: false}

	// TxApplyBfgObjectMapStream enables transactions for ApplyBfgObjectMapStream
	TxApplyBfgObjectMapStream = FeatureFlag{Name: "tx_apply_bfg_object_map_stream", OnByDefault: false}
	// TxResolveConflicts enables transactions for ResolveConflicts
	TxResolveConflicts = FeatureFlag{Name: "tx_resolve_conflicts", OnByDefault: false}
	// TxFetchIntoObjectPool enables transactions for FetchIntoObjectPool
	TxFetchIntoObjectPool = FeatureFlag{Name: "tx_fetch_into_object_pool", OnByDefault: false}
	// TxUserApplyPatch enables transactions for UserApplyPatch
	TxUserApplyPatch = FeatureFlag{Name: "tx_user_apply_patch", OnByDefault: false}
	// TxUserCherryPick enables transactions for UserCherryPick
	TxUserCherryPick = FeatureFlag{Name: "tx_user_cherry_pick", OnByDefault: false}
	// TxUserCommitFiles enables transactions for UserCommitFiles
	TxUserCommitFiles = FeatureFlag{Name: "tx_user_commit_files", OnByDefault: false}
	// TxUserFFBranch enables transactions for UserFFBranch
	TxUserFFBranch = FeatureFlag{Name: "tx_user_ff_branch", OnByDefault: false}
	// TxUserMergeBranch enables transactions for UserMergeBranch
	TxUserMergeBranch = FeatureFlag{Name: "tx_user_merge_branch", OnByDefault: false}
	// TxUserMergeToRef enables transactions for UserMergeToRef
	TxUserMergeToRef = FeatureFlag{Name: "tx_user_merge_to_ref", OnByDefault: false}
	// TxUserRebaseConfirmable enables transactions for UserRebaseConfirmable
	TxUserRebaseConfirmable = FeatureFlag{Name: "tx_user_rebase_confirmable", OnByDefault: false}
	// TxUserRevert enables transactions for UserRevert
	TxUserRevert = FeatureFlag{Name: "tx_user_revert", OnByDefault: false}
	// TxUserSquash enables transactions for UserSquash
	TxUserSquash = FeatureFlag{Name: "tx_user_squash", OnByDefault: false}
	// TxUserUpdateSubmodule enables transactions for UserUpdateSubmodule
	TxUserUpdateSubmodule = FeatureFlag{Name: "tx_user_update_submodule", OnByDefault: false}
	// TxDeleteRefs enables transactions for DeleteRefs
	TxDeleteRefs = FeatureFlag{Name: "tx_delete_refs", OnByDefault: false}
	// TxAddRemote enables transactions for AddRemote
	TxAddRemote = FeatureFlag{Name: "tx_add_remote", OnByDefault: false}
	// TxFetchInternalRemote enables transactions for FetchInternalRemote
	TxFetchInternalRemote = FeatureFlag{Name: "tx_fetch_internal_remote", OnByDefault: false}
	// TxRemoveRemote enables transactions for RemoveRemote
	TxRemoveRemote = FeatureFlag{Name: "tx_remove_remote", OnByDefault: false}
	// TxUpdateRemoteMirror enables transactions for UpdateRemoteMirror
	TxUpdateRemoteMirror = FeatureFlag{Name: "tx_update_remote_mirror", OnByDefault: false}
	// TxCloneFromPool enables transactions for CloneFromPool
	TxCloneFromPool = FeatureFlag{Name: "tx_clone_from_pool", OnByDefault: false}
	// TxCloneFromPoolInternal enables transactions for CloneFromPoolInternal
	TxCloneFromPoolInternal = FeatureFlag{Name: "tx_clone_from_pool_internal", OnByDefault: false}
	// TxCreateFork enables transactions for CreateFork
	TxCreateFork = FeatureFlag{Name: "tx_create_fork", OnByDefault: false}
	// TxCreateRepositoryFromBundle enables transactions for CreateRepositoryFromBundle
	TxCreateRepositoryFromBundle = FeatureFlag{Name: "tx_create_repository_from_bundle", OnByDefault: false}
	// TxCreateRepositoryFromSnapshot enables transactions for CreateRepositoryFromSnapshot
	TxCreateRepositoryFromSnapshot = FeatureFlag{Name: "tx_create_repository_from_snapshot", OnByDefault: false}
	// TxCreateRepositoryFromURL enables transactions for CreateRepositoryFromURL
	TxCreateRepositoryFromURL = FeatureFlag{Name: "tx_create_repository_from_u_r_l", OnByDefault: false}
	// TxFetchRemote enables transactions for FetchRemote
	TxFetchRemote = FeatureFlag{Name: "tx_fetch_remote", OnByDefault: false}
	// TxFetchSourceBranch enables transactions for FetchSourceBranch
	TxFetchSourceBranch = FeatureFlag{Name: "tx_fetch_source_branch", OnByDefault: false}
	// TxReplicateRepository enables transactions for ReplicateRepository
	TxReplicateRepository = FeatureFlag{Name: "tx_replicate_repository", OnByDefault: false}
	// TxWriteRef enables transactions for WriteRef
	TxWriteRef = FeatureFlag{Name: "tx_write_ref", OnByDefault: false}
	// TxWikiDeletePage enables transactions for WikiDeletePage
	TxWikiDeletePage = FeatureFlag{Name: "tx_wiki_delete_page", OnByDefault: false}
	// TxWikiUpdatePage enables transactions for WikiUpdatePage
	TxWikiUpdatePage = FeatureFlag{Name: "tx_wiki_update_page", OnByDefault: false}
	// TxWikiWritePage enables transactions for WikiWritePage
	TxWikiWritePage = FeatureFlag{Name: "tx_wiki_write_page", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoFetchSourceBranch,
	DistributedReads,
	LogCommandStats,
	ReferenceTransactions,
	GoUserFFBranch,
	GoUserCreateBranch,
	GoUserUpdateBranch,
	GoUserCommitFiles,
	GoResolveConflicts,
	GoUserUpdateSubmodule,
	GoUserCreateTag,
	GoUserRevert,
	TxApplyBfgObjectMapStream,
	TxResolveConflicts,
	TxFetchIntoObjectPool,
	TxUserApplyPatch,
	TxUserCherryPick,
	TxUserCommitFiles,
	TxUserFFBranch,
	TxUserMergeBranch,
	TxUserMergeToRef,
	TxUserRebaseConfirmable,
	TxUserRevert,
	TxUserSquash,
	TxUserUpdateSubmodule,
	TxDeleteRefs,
	TxAddRemote,
	TxFetchInternalRemote,
	TxRemoveRemote,
	TxUpdateRemoteMirror,
	TxCloneFromPool,
	TxCloneFromPoolInternal,
	TxCreateFork,
	TxCreateRepositoryFromBundle,
	TxCreateRepositoryFromSnapshot,
	TxCreateRepositoryFromURL,
	TxFetchRemote,
	TxFetchSourceBranch,
	TxReplicateRepository,
	TxWriteRef,
	TxWikiDeletePage,
	TxWikiUpdatePage,
	TxWikiWritePage,
}
