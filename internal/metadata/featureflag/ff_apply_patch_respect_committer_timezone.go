package featureflag

// ApplyPatchRespectCommitterTimezone changes UserApplyPatch to respect the commiter's timezone.
var ApplyPatchRespectCommitterTimezone = NewFeatureFlag("apply_patch_respect_committer_timezone", false)
