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
	GoFetchSourceBranch = FeatureFlag{Name: "go_fetch_source_branch", OnByDefault: false}
	// DistributedReads allows praefect to redirect accessor operations to up-to-date secondaries
	DistributedReads = FeatureFlag{Name: "distributed_reads", OnByDefault: false}
	// ReferenceTransactions will handle Git reference updates via the transaction service for strong consistency
	ReferenceTransactions = FeatureFlag{Name: "reference_transactions", OnByDefault: true}
	// ReferenceTranasctiionsPrimaryWins will change transaction registration such that
	// secondaries will take part in transactions, but not influence their outcome.
	ReferenceTransactionsPrimaryWins = FeatureFlag{Name: "reference_transactions_primary_wins", OnByDefault: false}
	// ReferenceTransactionHook will enable the reference-transaction hook
	// introduced with Git v2.28.0 for voting on transactions
	ReferenceTransactionHook = FeatureFlag{Name: "reference_transaction_hook", OnByDefault: true}
	// RubyReferenceTransactionHook will enable the reference-transaction hook
	// introduced with Git v2.28.0 for voting on transactions in the Ruby sidecar.
	RubyReferenceTransactionHook = FeatureFlag{Name: "ruby_reference_transaction_hook", OnByDefault: false}
	// LogCommandStats will log additional rusage stats for commands
	LogCommandStats = FeatureFlag{Name: "log_command_stats", OnByDefault: false}
	// GoUserMergeBranch enables the Go implementation of UserMergeBranch
	GoUserMergeBranch = FeatureFlag{Name: "go_user_merge_branch", OnByDefault: false}
	// GoUserMergeToRef enable the Go implementation of UserMergeToRef
	GoUserMergeToRef = FeatureFlag{Name: "go_user_merge_to_ref", OnByDefault: false}
	// GoUserFFBranch enables the Go implementation of UserFFBranch
	GoUserFFBranch = FeatureFlag{Name: "go_user_ff_branch", OnByDefault: false}
	// GoUserDeleteBranch enables the Go implementation of UserDeleteBranch
	GoUserDeleteBranch = FeatureFlag{Name: "go_user_delete_branch", OnByDefault: false}
	// GoUserSquash enable the Go implementation of UserSquash
	GoUserSquash = FeatureFlag{Name: "go_user_squash", OnByDefault: false}
	// GoListConflictFiles enables the Go implementation of ListConflictFiles
	GoListConflictFiles = FeatureFlag{Name: "go_list_conflict_files", OnByDefault: false}
	// GoResolveConflicts enables the Go implementation of ResolveConflicts
	GoResolveConflicts = FeatureFlag{Name: "go_resolve_conflicts", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoFetchSourceBranch,
	DistributedReads,
	ReferenceTransactions,
	ReferenceTransactionsPrimaryWins,
	ReferenceTransactionHook,
	RubyReferenceTransactionHook,
	GoUserMergeBranch,
	GoUserMergeToRef,
	GoUserFFBranch,
	GoUserDeleteBranch,
	GoUserSquash,
	GoListConflictFiles,
}

const (
	ReferenceTransactionHookEnvVar = "GITALY_REFERENCE_TRANSACTION_HOOK"
)
