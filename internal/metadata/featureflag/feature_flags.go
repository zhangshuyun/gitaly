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
	LogCommandStats = FeatureFlag{Name: "log_command_stats", OnByDefault: true}
	// GoUserUpdateBranch enables the Go implementation of UserUpdateBranch
	GoUserUpdateBranch = FeatureFlag{Name: "go_user_update_branch", OnByDefault: true}
	// GoResolveConflicts enables the Go implementation of ResolveConflicts
	GoResolveConflicts = FeatureFlag{Name: "go_resolve_conflicts", OnByDefault: false}
	// GoUserUpdateSubmodule enables the Go implementation of
	// UserUpdateSubmodules
	GoUserUpdateSubmodule = FeatureFlag{Name: "go_user_update_submodule", OnByDefault: true}
	// GoUserRevert enables the Go implementation of UserRevert
	GoUserRevert = FeatureFlag{Name: "go_user_revert", OnByDefault: false}
	// GoUpdateRemoteMirror enables the Go implementation of UpdateRemoteMirror
	GoUpdateRemoteMirror = FeatureFlag{Name: "go_update_remote_mirror", OnByDefault: false}
	// GrpcTreeEntryNotFound makes the TreeEntry gRPC call return NotFound instead of an empty blob
	GrpcTreeEntryNotFound = FeatureFlag{Name: "grpc_tree_entry_not_found", OnByDefault: false}
	// BackchannelVoting enables voting via the backchannel connection.
	BackchannelVoting = FeatureFlag{Name: "backchannel_voting", OnByDefault: false}
	// FetchInternalRemoteErrors makes FetchInternalRemote return actual errors instead of a boolean
	FetchInternalRemoteErrors = FeatureFlag{Name: "fetch_internal_remote_errors", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	LogCommandStats,
	ReferenceTransactions,
	GoUserUpdateBranch,
	GoResolveConflicts,
	GoUserUpdateSubmodule,
	GoUserRevert,
	GrpcTreeEntryNotFound,
	GoUpdateRemoteMirror,
	BackchannelVoting,
	FetchInternalRemoteErrors,
}
