package git

const (
	// scReadOnly denotes a read-only command
	scReadOnly = 1 << iota
	// scNoRefUpdates denotes a command which will never update refs
	scNoRefUpdates
	// scNoEndOfOptions denotes a command which doesn't know --end-of-options
	scNoEndOfOptions
	// scGeneratesPackfiles denotes a command which may generate packfiles
	scGeneratesPackfiles
)

// subcommands is a curated list of Git command names for special git.SafeCmd
// validation logic
var subcommands = map[string]uint{
	"apply":            scNoRefUpdates,
	"archive":          scReadOnly | scNoEndOfOptions,
	"blame":            scReadOnly | scNoEndOfOptions,
	"bundle":           scReadOnly | scGeneratesPackfiles,
	"cat-file":         scReadOnly,
	"checkout":         scNoEndOfOptions,
	"clone":            scNoEndOfOptions | scGeneratesPackfiles,
	"commit":           0,
	"commit-graph":     scNoRefUpdates,
	"config":           scNoRefUpdates | scNoEndOfOptions,
	"count-objects":    scReadOnly,
	"diff":             scReadOnly,
	"diff-tree":        scReadOnly,
	"fetch":            0,
	"for-each-ref":     scReadOnly | scNoEndOfOptions,
	"format-patch":     scReadOnly,
	"fsck":             scReadOnly,
	"gc":               scNoRefUpdates | scGeneratesPackfiles,
	"grep":             scReadOnly | scNoEndOfOptions,
	"hash-object":      scNoRefUpdates,
	"init":             scNoRefUpdates,
	"linguist":         scNoEndOfOptions,
	"log":              scReadOnly,
	"ls-remote":        scReadOnly,
	"ls-tree":          scReadOnly | scNoEndOfOptions,
	"merge-base":       scReadOnly,
	"multi-pack-index": scNoRefUpdates,
	"pack-refs":        scNoRefUpdates,
	"receive-pack":     0,
	"remote":           scNoEndOfOptions,
	"repack":           scNoRefUpdates | scGeneratesPackfiles,
	"rev-list":         scReadOnly,
	"rev-parse":        scReadOnly | scNoEndOfOptions,
	"show":             scReadOnly,
	"show-ref":         scReadOnly,
	"symbolic-ref":     0,
	"tag":              0,
	"update-ref":       0,
	"upload-archive":   scReadOnly | scNoEndOfOptions,
	"upload-pack":      scReadOnly | scGeneratesPackfiles,
	"worktree":         0,
}

// mayUpdateRef indicates if a subcommand is known to update references.
// This is useful to determine if a command requires reference hook
// configuration. A non-exhaustive list of commands is consulted to determine if
// refs are updated. When unknown, true is returned to err on the side of
// caution.
func mayUpdateRef(subcmd string) bool {
	flags, ok := subcommands[subcmd]
	if !ok {
		return true
	}
	return flags&(scReadOnly|scNoRefUpdates) == 0
}

// mayGeneratePackfiles indicates if a subcommand is known to generate
// packfiles. This is used in order to inject packfile configuration.
func mayGeneratePackfiles(subcmd string) bool {
	flags, ok := subcommands[subcmd]
	if !ok {
		return false
	}
	return flags&scGeneratesPackfiles != 0
}

// supportsEndOfOptions indicates whether a command can handle the
// `--end-of-options` option.
func supportsEndOfOptions(subcmd string) bool {
	flags, ok := subcommands[subcmd]
	if !ok {
		return true
	}
	return flags&scNoEndOfOptions == 0
}
