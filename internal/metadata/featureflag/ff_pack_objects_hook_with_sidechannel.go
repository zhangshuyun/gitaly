package featureflag

// PackObjectsHookWithSidechannel enables Unix socket sidechannels in 'gitaly-hooks git pack-objects'.
var PackObjectsHookWithSidechannel = NewFeatureFlag("pack_objects_hook_with_sidechannel", false)
