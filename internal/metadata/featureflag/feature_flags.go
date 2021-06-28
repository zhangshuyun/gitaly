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
	GoUpdateRemoteMirror = FeatureFlag{Name: "go_update_remote_mirror", OnByDefault: true}
	// FetchInternalRemoteErrors makes FetchInternalRemote return actual errors instead of a boolean
	FetchInternalRemoteErrors = FeatureFlag{Name: "fetch_internal_remote_errors", OnByDefault: false}
	// LFSPointersPipeline enables the alternative pipeline implementation of LFS-pointer
	// related RPCs.
	LFSPointersPipeline = FeatureFlag{Name: "lfs_pointers_pipeline", OnByDefault: false}
	// CreateRepositoryFromBundleAtomicFetch will add the `--atomic` flag to git-fetch(1) in
	// order to reduce the number of transactional votes.
	CreateRepositoryFromBundleAtomicFetch = FeatureFlag{Name: "create_repository_from_bundle_atomic_fetch", OnByDefault: false}
)

// All includes all feature flags.
var All = []FeatureFlag{
	GoUpdateRemoteMirror,
	FetchInternalRemoteErrors,
	LFSPointersPipeline,
	CreateRepositoryFromBundleAtomicFetch,
}
