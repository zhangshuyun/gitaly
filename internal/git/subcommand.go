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

type gitCommand struct {
	flags uint
	opts  []GlobalOption
}

// gitCommands is a curated list of Git command names for special git.NewCommand
// validation logic
var gitCommands = map[string]gitCommand{
	"apply": gitCommand{
		flags: scNoRefUpdates,
	},
	"archive": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"blame": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"bundle": gitCommand{
		flags: scReadOnly | scGeneratesPackfiles,
	},
	"cat-file": gitCommand{
		flags: scReadOnly,
	},
	"check-ref-format": gitCommand{
		flags: scReadOnly | scNoRefUpdates | scNoEndOfOptions,
	},
	"checkout": gitCommand{
		flags: scNoEndOfOptions,
	},
	"clone": gitCommand{
		flags: scNoEndOfOptions | scGeneratesPackfiles,
	},
	"commit": gitCommand{
		flags: 0,
	},
	"commit-graph": gitCommand{
		flags: scNoRefUpdates,
	},
	"config": gitCommand{
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"count-objects": gitCommand{
		flags: scReadOnly,
	},
	"diff": gitCommand{
		flags: scReadOnly,
	},
	"diff-tree": gitCommand{
		flags: scReadOnly,
	},
	"fetch": gitCommand{
		flags: 0,
	},
	"for-each-ref": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"format-patch": gitCommand{
		flags: scReadOnly,
	},
	"fsck": gitCommand{
		flags: scReadOnly,
	},
	"gc": gitCommand{
		flags: scNoRefUpdates | scGeneratesPackfiles,
	},
	"grep": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"hash-object": gitCommand{
		flags: scNoRefUpdates,
	},
	"init": gitCommand{
		flags: scNoRefUpdates,
	},
	"linguist": gitCommand{
		flags: scNoEndOfOptions,
	},
	"log": gitCommand{
		flags: scReadOnly,
	},
	"ls-remote": gitCommand{
		flags: scReadOnly,
	},
	"ls-tree": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"merge-base": gitCommand{
		flags: scReadOnly,
	},
	"mktag": gitCommand{
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"multi-pack-index": gitCommand{
		flags: scNoRefUpdates,
	},
	"pack-refs": gitCommand{
		flags: scNoRefUpdates,
	},
	"receive-pack": gitCommand{
		flags: 0,
		opts: []GlobalOption{
			// In case the repository belongs to an object pool, we want to prevent
			// Git from including the pool's refs in the ref advertisement. We do
			// this by rigging core.alternateRefsCommand to produce no output.
			// Because Git itself will append the pool repository directory, the
			// command ends with a "#". The end result is that Git runs `/bin/sh -c 'exit 0 # /path/to/pool.git`.
			ConfigPair{Key: "core.alternateRefsCommand", Value: "exit 0 #"},

			// In the past, there was a bug in git that caused users to
			// create commits with invalid timezones. As a result, some
			// histories contain commits that do not match the spec. As we
			// fsck received packfiles by default, any push containing such
			// a commit will be rejected. As this is a mostly harmless
			// issue, we add the following flag to ignore this check.
			ConfigPair{Key: "receive.fsck.badTimezone", Value: "ignore"},
		},
	},
	"remote": gitCommand{
		flags: scNoEndOfOptions,
	},
	"repack": gitCommand{
		flags: scNoRefUpdates | scGeneratesPackfiles,
	},
	"rev-list": gitCommand{
		flags: scReadOnly,
	},
	"rev-parse": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"show": gitCommand{
		flags: scReadOnly,
	},
	"show-ref": gitCommand{
		flags: scReadOnly,
	},
	"symbolic-ref": gitCommand{
		flags: 0,
	},
	"tag": gitCommand{
		flags: 0,
	},
	"update-ref": gitCommand{
		flags: 0,
	},
	"upload-archive": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"upload-pack": gitCommand{
		flags: scReadOnly | scGeneratesPackfiles,
		opts: []GlobalOption{
			ConfigPair{Key: "uploadpack.allowFilter", Value: "true"},
			// Enables the capability to request individual SHA1's from the
			// remote repo.
			ConfigPair{Key: "uploadpack.allowAnySHA1InWant", Value: "true"},
		},
	},
	"version": gitCommand{
		flags: scReadOnly | scNoEndOfOptions,
	},
	"worktree": gitCommand{
		flags: 0,
	},
}

// mayUpdateRef indicates if a gitCommand is known to update references.
// This is useful to determine if a command requires reference hook
// configuration. A non-exhaustive list of commands is consulted to determine if
// refs are updated. When unknown, true is returned to err on the side of
// caution.
func (c gitCommand) mayUpdateRef() bool {
	return c.flags&(scReadOnly|scNoRefUpdates) == 0
}

// mayGeneratePackfiles indicates if a gitCommand is known to generate
// packfiles. This is used in order to inject packfile configuration.
func (c gitCommand) mayGeneratePackfiles() bool {
	return c.flags&scGeneratesPackfiles != 0
}

// supportsEndOfOptions indicates whether a command can handle the
// `--end-of-options` option.
func (c gitCommand) supportsEndOfOptions() bool {
	return c.flags&scNoEndOfOptions == 0
}
