package stats

import (
	"context"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// HTTPPush hosts information about a typical HTTP-based push.
type HTTPPush struct {
	// SendPack is the upload of the packfile performed as part of the clone.
	SendPack HTTPSendPack
}

// PushCommand is a command updating remote references.
type PushCommand struct {
	// Reference is the reference that shall be updated.
	Reference git.ReferenceName
	// OldOID is the expected state of the reference before being updated.
	OldOID git.ObjectID
	// NewOID is the expected state of the reference after being updated.
	NewOID git.ObjectID
}

// PerformHTTPPush emulates a git-push(1) over the HTTP protocol.
func PerformHTTPPush(
	ctx context.Context,
	url, user, password string,
	commands []PushCommand,
	packfile io.Reader,
	interactive bool,
) (HTTPPush, error) {
	printInteractive := func(format string, a ...interface{}) {
		if interactive {
			// Ignore any errors returned by this given that we only use it as a
			// debugging aid to write to stdout.
			fmt.Printf(format, a...)
		}
	}

	sendPack, err := performHTTPSendPack(ctx, url, user, password,
		commands, packfile, printInteractive)
	if err != nil {
		return HTTPPush{}, ctxErr(ctx, err)
	}

	return HTTPPush{
		SendPack: sendPack,
	}, nil
}
