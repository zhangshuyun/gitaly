package featureflag

// UseBundledGit enables the use of bundled Git if the Gitaly configuration has both a binary path
// and the bundled Git enabled.
var UseBundledGit = NewFeatureFlag("use_bundled_git", false)
