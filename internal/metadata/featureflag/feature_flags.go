package featureflag

import (
	"fmt"
	"regexp"
)

// UploadPackFilter enables partial clones by sending uploadpack.allowFilter and uploadpack.allowAnySHA1InWant
// to upload-pack
//
// LinguistFileCountStats will invoke an additional git-linguist command to get the number of files per language
//
// HooksRPC will invoke update, pre receive, and post receive hooks by using RPCs
//
// CacheInvalidator controls the tracking of repo state via gRPC
// annotations (i.e. accessor and mutator RPC's). This enables cache
// invalidation by changing state when the repo is modified.
var (
	UploadPackFilter       = mustValidateFF("upload_pack_filter")
	LinguistFileCountStats = mustValidateFF("linguist_file_count_stats")
	HooksRPC               = mustValidateFF("hooks_rpc")
	CacheInvalidator       = mustValidateFF("cache_invalidator")
)

const (
	// HooksRPCEnvVar is the name of the environment variable we use to pass the feature flag down into gitaly-hooks
	HooksRPCEnvVar = "GITALY_HOOK_RPCS_ENABLED"
)

var ffRegex = regexp.MustCompile(`^[[:alpha:]_]+$`)

func mustValidateFF(ff string) string {
	if !ffRegex.MatchString(ff) {
		panic(fmt.Sprintf("invalid chars found in feature flag: %q", ff))
	}
	return ff
}
