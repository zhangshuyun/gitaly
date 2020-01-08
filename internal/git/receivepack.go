package git

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// ReceivePackRequest abstracts away the different requests that end up
// spawning git-receive-pack.
type ReceivePackRequest interface {
	GetGlId() string
	GetGlUsername() string
	GetGlRepository() string
	GetRepository() *gitalypb.Repository
}

// HookEnv is information we pass down to the Git hooks during
// git-receive-pack.
func HookEnv(req ReceivePackRequest) []string {
	return append([]string{
		fmt.Sprintf("GL_ID=%s", req.GetGlId()),
		fmt.Sprintf("GL_USERNAME=%s", req.GetGlUsername()),
		fmt.Sprintf("GL_REPOSITORY=%s", req.GetGlRepository()),
		fmt.Sprintf("GITALY_SOCKET=" + config.GitalyInternalSocketPath()),
		fmt.Sprintf("GL_REPO_STORAGE=%s", req.GetRepository().GetStorageName()),
		fmt.Sprintf("GL_REPO_RELATIVE_PATH=%s", req.GetRepository().GetRelativePath()),
	}, gitlabshell.Env()...)
}

// ReceivePackConfig contains config options we want to enforce when
// receiving a push with git-receive-pack.
func ReceivePackConfig() []Option {
	return []Option{
		ValueFlag{"-c", fmt.Sprintf("core.hooksPath=%s", hooks.Path())},

		// In case the repository belongs to an object pool, we want to prevent
		// Git from including the pool's refs in the ref advertisement. We do
		// this by rigging core.alternateRefsCommand to produce no output.
		// Because Git itself will append the pool repository directory, the
		// command ends with a "#". The end result is that Git runs `/bin/sh -c 'exit 0 # /path/to/pool.git`.
		ValueFlag{"-c", "core.alternateRefsCommand=exit 0 #"},
	}
}
