package featureflag

// HooksInTempdir switches away from Ruby hooks to hooks stored in a temporary directory. Every
// hook is simply a symlink to the gitaly-hooks binary.
var HooksInTempdir = NewFeatureFlag("hooks_in_tempdir", true)
