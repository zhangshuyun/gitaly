package featureflag

// TxApplyGitattributesTwoPhaseRemoval enables two-phase voting on the removal of the gitattributes
// file.
var TxApplyGitattributesTwoPhaseRemoval = NewFeatureFlag("tx_apply_gitattributes_two_phase_removal", false)
