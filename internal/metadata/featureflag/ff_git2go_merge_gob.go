package featureflag

// Git2GoMergeGob switches gitaly-git2go's merge command to use gob for serialization. This change
// allows Gitaly to pass through errors with additional details, for example to describe the set of
// conflicting files.
var Git2GoMergeGob = NewFeatureFlag("git2go_merge_gob", false)
