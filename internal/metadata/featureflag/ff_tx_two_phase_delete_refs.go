package featureflag

// TxTwoPhaseDeleteRefs enables two-phase voting for the DeleteRefs RPC.
var TxTwoPhaseDeleteRefs = NewFeatureFlag("tx_two_phase_delete_refs", false)
