package featureflag

// RunCommandsInCGroup allows all commands to be run within a cgroup
var RunCommandsInCGroup = NewFeatureFlag("run_cmds_in_cgroup", true)
