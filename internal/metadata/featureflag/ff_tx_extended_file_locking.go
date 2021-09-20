package featureflag

// TxExtendedFileLocking enables two-phase voting on files with proper locking semantics such that
// no races can exist anymore in more places.
var TxExtendedFileLocking = NewFeatureFlag("tx_extended_file_locking", false)
