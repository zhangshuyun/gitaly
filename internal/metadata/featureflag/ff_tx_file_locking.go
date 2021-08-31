package featureflag

// TxFileLocking enables two-phase voting on files with proper locking semantics such that no races
// can exist anymore.
var TxFileLocking = NewFeatureFlag("tx_file_locking", false)
