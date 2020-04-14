package git

import (
	"fmt"

	"github.com/golang/protobuf/jsonpb"
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

var jsonpbMarshaller = &jsonpb.Marshaler{}

// HookEnv is information we pass down to the Git hooks during
// git-receive-pack.
func HookEnv(req ReceivePackRequest) ([]string, error) {
	repo, err := jsonpbMarshaller.MarshalToString(req.GetRepository())
	if err != nil {
		return nil, err
	}

	gitlabshellEnv, err := gitlabshell.Env()
	if err != nil {
		return nil, err
	}
	return append([]string{
		fmt.Sprintf("GL_ID=%s", req.GetGlId()),
		fmt.Sprintf("GL_USERNAME=%s", req.GetGlUsername()),
		fmt.Sprintf("GL_REPOSITORY=%s", req.GetGlRepository()),
		fmt.Sprintf("GITALY_SOCKET=" + config.GitalyInternalSocketPath()),
		fmt.Sprintf("GITALY_REPO=%s", repo),
	}, gitlabshellEnv...), nil
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

		// In the past, there was a bug in git that caused users to
		// create commits with invalid timezones. As a result, some
		// histories contain commits that do not match the spec. As we
		// fsck received packfiles by default, any push containing such
		// a commit will be rejected. As this is a mostly harmless
		// issue, we add the following flag to ignore this check.
		ValueFlag{"-c", "receive.fsck.badTimezone=ignore"},
	}
}
