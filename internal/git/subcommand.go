package git

// Curated list of Git command names for special git.SafeCmd validation logic
const (
	scCatFile        = "cat-file"
	scLog            = "log"
	scForEachRef     = "for-each-ref"
	scRevParse       = "rev-parse"
	scCountObjects   = "count-objects"
	scConfig         = "config"
	scMultiPackIndex = "multi-pack-index"
	scRepack         = "repack"
	scDiff           = "diff"
	scPackRefs       = "pack-refs"
	scMergeBase      = "merge-base"
	scHashObject     = "hash-object"
	scShowRef        = "show-ref"
	scUploadPack     = "upload-pack"
	scUploadArchive  = "upload-archive"
	scBlame          = "blame"
	scLsTree         = "ls-tree"
	scRevList        = "rev-list"
	scLsRemote       = "ls-remote"
	scFsck           = "fsck"
	scGrep           = "grep"
	scBundle         = "bundle"
	scArchive        = "archive"
	scFormatPatch    = "format-patch"
)

var knownReadOnlyCmds = map[string]struct{}{
	scCatFile:       struct{}{},
	scLog:           struct{}{},
	scForEachRef:    struct{}{},
	scRevParse:      struct{}{},
	scCountObjects:  struct{}{},
	scDiff:          struct{}{},
	scMergeBase:     struct{}{},
	scShowRef:       struct{}{},
	scUploadPack:    struct{}{},
	scUploadArchive: struct{}{},
	scBlame:         struct{}{},
	scLsTree:        struct{}{},
	scRevList:       struct{}{},
	scLsRemote:      struct{}{},
	scFsck:          struct{}{},
	scGrep:          struct{}{},
	scBundle:        struct{}{},
	scArchive:       struct{}{},
	scFormatPatch:   struct{}{},
}

// knownNoRefUpdates indicates all repo mutating commands where it is known
// whether references are never updated
var knownNoRefUpdates = map[string]struct{}{
	scConfig:         struct{}{},
	scMultiPackIndex: struct{}{},
	scRepack:         struct{}{},
	scPackRefs:       struct{}{},
	scHashObject:     struct{}{},
}

// mayUpdateRef indicates if a subcommand is known to update references.
// This is useful to determine if a command requires reference hook
// configuration. A non-exhaustive list of commands is consulted to determine if
// refs are updated. When unknown, true is returned to err on the side of
// caution.
func mayUpdateRef(subcmd string) bool {
	if _, ok := knownReadOnlyCmds[subcmd]; ok {
		return false
	}
	if _, ok := knownNoRefUpdates[subcmd]; ok {
		return false
	}
	return true
}
