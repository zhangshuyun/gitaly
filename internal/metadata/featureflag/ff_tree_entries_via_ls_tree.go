package featureflag

// TreeEntriesViaLsTree switches over retrieval of tree entries from using git-cat-file(1) to
// instead use git-ls-tree(1).
var TreeEntriesViaLsTree = NewFeatureFlag("tree_entries_via_ls_tree", false)
