package featureflag

type FeatureFlag struct {
	Name        string `json:"name"`
	OnByDefault bool   `json:"on_by_default"`
}

// A set of feature flags used in Gitaly and Praefect.
// In order to support coverage of combined features usage all feature flags should be marked as enabled for the test.
// NOTE: if you add a new feature flag please add it to the `All` list defined below.
var (
	// GoUpdateRemoteMirror enables the Go implementation of UpdateRemoteMirror
	GoUpdateRemoteMirror = FeatureFlag{Name: "go_update_remote_mirror", OnByDefault: false}
	// FetchInternalRemoteErrors makes FetchInternalRemote return actual errors instead of a boolean
	FetchInternalRemoteErrors = FeatureFlag{Name: "fetch_internal_remote_errors", OnByDefault: false}
	// TxConfig enables transactional voting for SetConfig and DeleteConfig RPCs.
	TxConfig = FeatureFlag{Name: "tx_config", OnByDefault: true}
	// TxRemote enables transactional voting for AddRemote and DeleteRemote.
	TxRemote = FeatureFlag{Name: "tx_remote", OnByDefault: true}
	// UserMergeToRefSkipPrecursorRefUpdate causes UserMergeToRef to not update the
	// target reference in case computing the merge fails.
	UserMergeToRefSkipPrecursorRefUpdate = FeatureFlag{Name: "user_merge_to_ref_skip_precursor_ref_update", OnByDefault: true}
	// GoFindLicense enables Go implementation of FindLicense
	GoFindLicense = FeatureFlag{Name: "go_find_license", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoUpdateRemoteMirror,
	FetchInternalRemoteErrors,
	TxConfig,
	TxRemote,
	UserMergeToRefSkipPrecursorRefUpdate,
	GoFindLicense,
}
