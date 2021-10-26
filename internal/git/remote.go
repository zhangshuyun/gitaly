package git

import (
	"context"
)

// Remote represents 'remote' sub-command.
// https://git-scm.com/docs/git-remote
type Remote interface {
	// Add creates a new remote repository if it doesn't exist.
	// If such a remote already exists it returns an ErrAlreadyExists error.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emaddem
	Add(ctx context.Context, name, url string, opts RemoteAddOpts) error
	// Remove removes the remote configured for the local repository and all configurations associated with it.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emremoveem
	Remove(ctx context.Context, name string) error
	// SetURL sets a new url value for an existing remote.
	// If remote doesn't exist it returns an ErrNotFound error.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emset-urlem
	SetURL(ctx context.Context, name, url string, opts SetURLOpts) error
}

// RemoteAddOptsMirror represents possible values for the '--mirror' flag value
type RemoteAddOptsMirror string

func (m RemoteAddOptsMirror) String() string {
	return string(m)
}

var (
	// RemoteAddOptsMirrorDefault allows to use a default behaviour.
	RemoteAddOptsMirrorDefault = RemoteAddOptsMirror("")
	// RemoteAddOptsMirrorFetch configures everything in refs/ on the remote to be
	// directly mirrored into refs/ in the local repository.
	RemoteAddOptsMirrorFetch = RemoteAddOptsMirror("fetch")
	// RemoteAddOptsMirrorPush configures 'git push' to always behave as if --mirror was passed.
	RemoteAddOptsMirrorPush = RemoteAddOptsMirror("push")
)

// RemoteAddOptsTags controls whether tags will be fetched.
type RemoteAddOptsTags string

func (t RemoteAddOptsTags) String() string {
	return string(t)
}

var (
	// RemoteAddOptsTagsDefault enables importing of tags only on fetched branches.
	RemoteAddOptsTagsDefault = RemoteAddOptsTags("")
	// RemoteAddOptsTagsAll enables importing of every tag from the remote repository.
	RemoteAddOptsTagsAll = RemoteAddOptsTags("--tags")
	// RemoteAddOptsTagsNone disables importing of tags from the remote repository.
	RemoteAddOptsTagsNone = RemoteAddOptsTags("--no-tags")
)

// RemoteAddOpts is used to configure invocation of the 'git remote add' command.
// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emaddem
type RemoteAddOpts struct {
	// RemoteTrackingBranches controls what branches should be tracked instead of
	// all branches which is a default refs/remotes/<name>.
	// For each entry the refspec '+refs/heads/<branch>:refs/remotes/<remote>/<branch>' would be created and added to the configuration.
	RemoteTrackingBranches []string
	// DefaultBranch sets the default branch (i.e. the target of the symbolic-ref refs/remotes/<name>/HEAD)
	// for the named remote.
	// If set to 'develop' then: 'git symbolic-ref refs/remotes/<remote>/HEAD' call will result to 'refs/remotes/<remote>/develop'.
	DefaultBranch string
	// Fetch controls if 'git fetch <name>' is run immediately after the remote information is set up.
	Fetch bool
	// Tags controls whether tags will be fetched as part of the remote or not.
	Tags RemoteAddOptsTags
	// Mirror controls value used for '--mirror' flag.
	Mirror RemoteAddOptsMirror
}

// SetURLOpts are the options for SetURL.
type SetURLOpts struct {
	// Push URLs are manipulated instead of fetch URLs.
	Push bool
}
