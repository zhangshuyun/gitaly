package git

const (
	// scNoRefUpdates denotes a command which will never update refs
	scNoRefUpdates = 1 << iota
	// scNoEndOfOptions denotes a command which doesn't know --end-of-options
	scNoEndOfOptions
	// scGeneratesPackfiles denotes a command which may generate packfiles
	scGeneratesPackfiles
)

type commandDescription struct {
	flags uint
	opts  []GlobalOption
}

// commandDescriptions is a curated list of Git command descriptions for special
// git.ExecCommandFactory validation logic
var commandDescriptions = map[string]commandDescription{
	"apply": {
		flags: scNoRefUpdates,
	},
	"archive": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"blame": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"bundle": {
		flags: scNoRefUpdates | scGeneratesPackfiles,
	},
	"cat-file": {
		flags: scNoRefUpdates,
	},
	"check-ref-format": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"checkout": {
		flags: scNoEndOfOptions,
	},
	"clone": {
		flags: scNoEndOfOptions | scGeneratesPackfiles,
	},
	"commit": {
		flags: 0,
	},
	"commit-graph": {
		flags: scNoRefUpdates,
	},
	"config": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"count-objects": {
		flags: scNoRefUpdates,
	},
	"diff": {
		flags: scNoRefUpdates,
	},
	"diff-tree": {
		flags: scNoRefUpdates,
	},
	"fetch": {
		flags: 0,
	},
	"for-each-ref": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"format-patch": {
		flags: scNoRefUpdates,
	},
	"fsck": {
		flags: scNoRefUpdates,
	},
	"gc": {
		flags: scNoRefUpdates | scGeneratesPackfiles,
	},
	"grep": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"hash-object": {
		flags: scNoRefUpdates,
	},
	"init": {
		flags: scNoRefUpdates,
	},
	"linguist": {
		flags: scNoEndOfOptions,
	},
	"log": {
		flags: scNoRefUpdates,
	},
	"ls-remote": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"ls-tree": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"merge-base": {
		flags: scNoRefUpdates,
	},
	"mktag": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"multi-pack-index": {
		flags: scNoRefUpdates,
	},
	"pack-refs": {
		flags: scNoRefUpdates,
	},
	"pack-objects": {
		flags: scNoRefUpdates | scGeneratesPackfiles,
	},
	"push": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"receive-pack": {
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

			// Make git-receive-pack(1) advertise the push options
			// capability to clients.
			ConfigPair{Key: "receive.advertisePushOptions", Value: "true"},
		},
	},
	"remote": {
		flags: scNoEndOfOptions,
	},
	"repack": {
		flags: scNoRefUpdates | scGeneratesPackfiles,
		opts: []GlobalOption{
			// Write bitmap indices when packing objects, which
			// speeds up packfile creation for fetches.
			ConfigPair{Key: "repack.writeBitmaps", Value: "true"},
		},
	},
	"rev-list": {
		flags: scNoRefUpdates,
	},
	"rev-parse": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"show": {
		flags: scNoRefUpdates,
	},
	"show-ref": {
		flags: scNoRefUpdates,
	},
	"symbolic-ref": {
		flags: 0,
	},
	"tag": {
		flags: 0,
	},
	"update-ref": {
		flags: 0,
	},
	"upload-archive": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"upload-pack": {
		flags: scNoRefUpdates | scGeneratesPackfiles,
		opts: []GlobalOption{
			ConfigPair{Key: "uploadpack.allowFilter", Value: "true"},
			// Enables the capability to request individual SHA1's from the
			// remote repo.
			ConfigPair{Key: "uploadpack.allowAnySHA1InWant", Value: "true"},
		},
	},
	"version": {
		flags: scNoRefUpdates | scNoEndOfOptions,
	},
	"worktree": {
		flags: 0,
	},
}

// mayUpdateRef indicates if a command is known to update references.
// This is useful to determine if a command requires reference hook
// configuration. A non-exhaustive list of commands is consulted to determine if
// refs are updated. When unknown, true is returned to err on the side of
// caution.
func (c commandDescription) mayUpdateRef() bool {
	return c.flags&scNoRefUpdates == 0
}

// mayGeneratePackfiles indicates if a command is known to generate
// packfiles. This is used in order to inject packfile configuration.
func (c commandDescription) mayGeneratePackfiles() bool {
	return c.flags&scGeneratesPackfiles != 0
}

// supportsEndOfOptions indicates whether a command can handle the
// `--end-of-options` option.
func (c commandDescription) supportsEndOfOptions() bool {
	return c.flags&scNoEndOfOptions == 0
}
