package featureflag

type FeatureFlag struct {
	Name        string `json:"name"`
	OnByDefault bool   `json:"on_by_default"`
}

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// DistributedReads allows praefect to redirect accessor operations to up-to-date secondaries
	DistributedReads = FeatureFlag{Name: "distributed_reads", OnByDefault: true}
	// ReferenceTransactions will handle Git reference updates via the transaction service for strong consistency
	ReferenceTransactions = FeatureFlag{Name: "reference_transactions", OnByDefault: true}
	// LogCommandStats will log additional rusage stats for commands
	LogCommandStats = FeatureFlag{Name: "log_command_stats", OnByDefault: false}
	// GoUserCherryPick enables the Go implementation of UserCherryPick
	GoUserCherryPick = FeatureFlag{Name: "go_user_cherry_pick", OnByDefault: false}
	// GoUserUpdateBranch enables the Go implementation of UserUpdateBranch
	GoUserUpdateBranch = FeatureFlag{Name: "go_user_update_branch", OnByDefault: false}
	// GoUserCommitFiles enables the Go implementation of UserCommitFiles
	GoUserCommitFiles = FeatureFlag{Name: "go_user_commit_files", OnByDefault: true}
	// GoResolveConflicts enables the Go implementation of ResolveConflicts
	GoResolveConflicts = FeatureFlag{Name: "go_resolve_conflicts", OnByDefault: false}
	// GoUserUpdateSubmodule enables the Go implementation of
	// UserUpdateSubmodules
	GoUserUpdateSubmodule = FeatureFlag{Name: "go_user_update_submodule", OnByDefault: false}
	// GoUserRevert enables the Go implementation of UserRevert
	GoUserRevert = FeatureFlag{Name: "go_user_revert", OnByDefault: false}
	// GoGetAllLFSPointers enables the Go implementation of GetAllLFSPointers
	GoGetAllLFSPointers = FeatureFlag{Name: "go_get_all_lfs_pointers", OnByDefault: false}
	// GoGetLFSPointers enables the Go implementation of GetLFSPointers
	GoGetLFSPointers = FeatureFlag{Name: "go_get_lfs_pointers", OnByDefault: false}
	// GoGetNewPointers enables the Go implementation of GetNewLFSPointers
	GoGetNewLFSPointers = FeatureFlag{Name: "go_get_new_lfs_pointers", OnByDefault: false}
	// UploadPackGitalyHooks makes git-upload-pack use gitaly-hooks to run pack-objects
	UploadPackGitalyHooks = FeatureFlag{Name: "upload_pack_gitaly_hooks", OnByDefault: false}

	// TxApplyBfgObjectMapStream enables transactions for ApplyBfgObjectMapStream
	TxApplyBfgObjectMapStream = FeatureFlag{Name: "tx_apply_bfg_object_map_stream", OnByDefault: true}
	// TxApplyGitattributes enables transactions for ApplyGitattributes
	TxApplyGitattributes = FeatureFlag{Name: "tx_apply_gitattributes", OnByDefault: true}
	// TxResolveConflicts enables transactions for ResolveConflicts
	TxResolveConflicts = FeatureFlag{Name: "tx_resolve_conflicts", OnByDefault: true}
	// TxFetchIntoObjectPool enables transactions for FetchIntoObjectPool
	TxFetchIntoObjectPool = FeatureFlag{Name: "tx_fetch_into_object_pool", OnByDefault: true}
	// TxUserApplyPatch enables transactions for UserApplyPatch
	TxUserApplyPatch = FeatureFlag{Name: "tx_user_apply_patch", OnByDefault: true}
	// TxUserCherryPick enables transactions for UserCherryPick
	TxUserCherryPick = FeatureFlag{Name: "tx_user_cherry_pick", OnByDefault: true}
	// TxUserCommitFiles enables transactions for UserCommitFiles
	TxUserCommitFiles = FeatureFlag{Name: "tx_user_commit_files", OnByDefault: true}
	// TxUserFFBranch enables transactions for UserFFBranch
	TxUserFFBranch = FeatureFlag{Name: "tx_user_ff_branch", OnByDefault: true}
	// TxUserMergeBranch enables transactions for UserMergeBranch
	TxUserMergeBranch = FeatureFlag{Name: "tx_user_merge_branch", OnByDefault: true}
	// TxUserMergeToRef enables transactions for UserMergeToRef
	TxUserMergeToRef = FeatureFlag{Name: "tx_user_merge_to_ref", OnByDefault: true}
	// TxUserRebaseConfirmable enables transactions for UserRebaseConfirmable
	TxUserRebaseConfirmable = FeatureFlag{Name: "tx_user_rebase_confirmable", OnByDefault: true}
	// TxUserRevert enables transactions for UserRevert
	TxUserRevert = FeatureFlag{Name: "tx_user_revert", OnByDefault: true}
	// TxUserSquash enables transactions for UserSquash
	TxUserSquash = FeatureFlag{Name: "tx_user_squash", OnByDefault: true}
	// TxUserUpdateSubmodule enables transactions for UserUpdateSubmodule
	TxUserUpdateSubmodule = FeatureFlag{Name: "tx_user_update_submodule", OnByDefault: true}
	// TxDeleteRefs enables transactions for DeleteRefs
	TxDeleteRefs = FeatureFlag{Name: "tx_delete_refs", OnByDefault: true}
	// TxAddRemote enables transactions for AddRemote
	TxAddRemote = FeatureFlag{Name: "tx_add_remote", OnByDefault: true}
	// TxFetchInternalRemote enables transactions for FetchInternalRemote
	TxFetchInternalRemote = FeatureFlag{Name: "tx_fetch_internal_remote", OnByDefault: true}
	// TxRemoveRemote enables transactions for RemoveRemote
	TxRemoveRemote = FeatureFlag{Name: "tx_remove_remote", OnByDefault: true}
	// TxUpdateRemoteMirror enables transactions for UpdateRemoteMirror
	TxUpdateRemoteMirror = FeatureFlag{Name: "tx_update_remote_mirror", OnByDefault: true}
	// TxCloneFromPool enables transactions for CloneFromPool
	TxCloneFromPool = FeatureFlag{Name: "tx_clone_from_pool", OnByDefault: true}
	// TxCloneFromPoolInternal enables transactions for CloneFromPoolInternal
	TxCloneFromPoolInternal = FeatureFlag{Name: "tx_clone_from_pool_internal", OnByDefault: true}
	// TxCreateFork enables transactions for CreateFork
	TxCreateFork = FeatureFlag{Name: "tx_create_fork", OnByDefault: true}
	// TxCreateRepositoryFromBundle enables transactions for CreateRepositoryFromBundle
	TxCreateRepositoryFromBundle = FeatureFlag{Name: "tx_create_repository_from_bundle", OnByDefault: true}
	// TxCreateRepositoryFromSnapshot enables transactions for CreateRepositoryFromSnapshot
	TxCreateRepositoryFromSnapshot = FeatureFlag{Name: "tx_create_repository_from_snapshot", OnByDefault: true}
	// TxCreateRepositoryFromURL enables transactions for CreateRepositoryFromURL
	TxCreateRepositoryFromURL = FeatureFlag{Name: "tx_create_repository_from_u_r_l", OnByDefault: true}
	// TxFetchRemote enables transactions for FetchRemote
	TxFetchRemote = FeatureFlag{Name: "tx_fetch_remote", OnByDefault: true}
	// TxFetchSourceBranch enables transactions for FetchSourceBranch
	TxFetchSourceBranch = FeatureFlag{Name: "tx_fetch_source_branch", OnByDefault: true}
	// TxReplicateRepository enables transactions for ReplicateRepository
	TxReplicateRepository = FeatureFlag{Name: "tx_replicate_repository", OnByDefault: true}
	// TxWriteRef enables transactions for WriteRef
	TxWriteRef = FeatureFlag{Name: "tx_write_ref", OnByDefault: true}
	// TxWikiDeletePage enables transactions for WikiDeletePage
	TxWikiDeletePage = FeatureFlag{Name: "tx_wiki_delete_page", OnByDefault: true}
	// TxWikiUpdatePage enables transactions for WikiUpdatePage
	TxWikiUpdatePage = FeatureFlag{Name: "tx_wiki_update_page", OnByDefault: true}
	// TxWikiWritePage enables transactions for WikiWritePage
	TxWikiWritePage = FeatureFlag{Name: "tx_wiki_write_page", OnByDefault: true}
)

// All includes all feature flags.
var All = []FeatureFlag{
	DistributedReads,
	LogCommandStats,
	ReferenceTransactions,
	GoUserCherryPick,
	GoUserUpdateBranch,
	GoUserCommitFiles,
	GoResolveConflicts,
	GoUserUpdateSubmodule,
	GoUserRevert,
	GoGetAllLFSPointers,
	GoGetLFSPointers,
	GoGetNewLFSPointers,
	TxApplyBfgObjectMapStream,
	TxApplyGitattributes,
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
	UploadPackGitalyHooks,
}
