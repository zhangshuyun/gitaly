package featureflag

type FeatureFlag struct {
	Name        string `json:"name"`
	OnByDefault bool   `json:"on_by_default"`
}

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// ReferenceTransactions will handle Git reference updates via the transaction service for strong consistency
	ReferenceTransactions = FeatureFlag{Name: "reference_transactions", OnByDefault: true}
	// LogCommandStats will log additional rusage stats for commands
	LogCommandStats = FeatureFlag{Name: "log_command_stats", OnByDefault: false}
	// GoUserCherryPick enables the Go implementation of UserCherryPick
	GoUserCherryPick = FeatureFlag{Name: "go_user_cherry_pick", OnByDefault: false}
	// GoUserUpdateBranch enables the Go implementation of UserUpdateBranch
	GoUserUpdateBranch = FeatureFlag{Name: "go_user_update_branch", OnByDefault: true}
	// GoResolveConflicts enables the Go implementation of ResolveConflicts
	GoResolveConflicts = FeatureFlag{Name: "go_resolve_conflicts", OnByDefault: false}
	// GoUserUpdateSubmodule enables the Go implementation of
	// UserUpdateSubmodules
	GoUserUpdateSubmodule = FeatureFlag{Name: "go_user_update_submodule", OnByDefault: true}
	// GoUserRevert enables the Go implementation of UserRevert
	GoUserRevert = FeatureFlag{Name: "go_user_revert", OnByDefault: false}
	// LFSPointersUseBitmapIndex enables the use of bitmap indices when searching LFS pointers.
	LFSPointersUseBitmapIndex = FeatureFlag{Name: "lfs_pointers_use_bitmap_index", OnByDefault: false}
	// GoUpdateRemoteMirror enables the Go implementation of UpdateRemoteMirror
	GoUpdateRemoteMirror = FeatureFlag{Name: "go_update_remote_mirror", OnByDefault: false}
	// ConnectionMultiplexing enables the use of multiplexed connection from Praefect to Gitaly.
	ConnectionMultiplexing = FeatureFlag{Name: "connection_multiplexing", OnByDefault: false}
	// PackWriteReverseIndex enables writing of the reverse index for newly written packfiles,
	// which is supposed to speed up computation of object sizes.
	PackWriteReverseIndex = FeatureFlag{Name: "pack_write_reverse_index", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	LogCommandStats,
	ReferenceTransactions,
	GoUserCherryPick,
	GoUserUpdateBranch,
	GoResolveConflicts,
	GoUserUpdateSubmodule,
	GoUserRevert,
	LFSPointersUseBitmapIndex,
	GoUpdateRemoteMirror,
	ConnectionMultiplexing,
	PackWriteReverseIndex,
}
