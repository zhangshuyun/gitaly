package hook

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"time"

	gitaly_metadata "gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"google.golang.org/grpc/metadata"
)

const (
	sidechannelHeader = "gitaly-sidechannel-socket"
	sidechannelSocket = "sidechannel"
)

type errInvalidSidechannelAddress struct{ string }

func (e *errInvalidSidechannelAddress) Error() string {
	return fmt.Sprintf("invalid side channel address: %q", e.string)
}

// GetSidechannel looks for a sidechannel address in an incoming context
// and establishes a connection if it finds an address.
func GetSidechannel(ctx context.Context) (net.Conn, error) {
	address := gitaly_metadata.GetValue(ctx, sidechannelHeader)
	if path.Base(address) != sidechannelSocket {
		return nil, &errInvalidSidechannelAddress{address}
	}

	return net.DialTimeout("unix", address, time.Second)
}

// SetupSidechannel creates a sidechannel listener in a tempdir and
// launches a goroutine that will run the callback if the listener
// receives a connection. The address of the listener is stored in the
// returned context, so that the caller can propagate it to a server. The
// caller must Close the SidechannelWaiter to prevent resource leaks.
func SetupSidechannel(ctx context.Context, callback func(*net.UnixConn) error) (_ context.Context, _ *SidechannelWaiter, err error) {
	socketDir, err := os.MkdirTemp("", "gitaly")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(socketDir)
		}
	}()

	address := path.Join(socketDir, sidechannelSocket)
	l, err := net.ListenUnix("unix", &net.UnixAddr{Net: "unix", Name: address})
	if err != nil {
		return nil, nil, err
	}

	wt := &SidechannelWaiter{
		errC:      make(chan error),
		socketDir: socketDir,
		listener:  l,
	}
	go wt.run(callback)

	ctx = metadata.AppendToOutgoingContext(ctx, sidechannelHeader, address)
	return ctx, wt, nil
}

// SidechannelWaiter provides cleanup and error propagation for a
// sidechannel callback.
type SidechannelWaiter struct {
	errC      chan error
	socketDir string
	listener  *net.UnixListener
}

func (wt *SidechannelWaiter) run(callback func(*net.UnixConn) error) {
	defer close(wt.errC)

	wt.errC <- func() error {
		c, err := wt.listener.AcceptUnix()
		if err != nil {
			return err
		}
		defer c.Close()

		// Eagerly remove the socket directory, in case the process exits before
		// wt.Close() can run.
		if err := os.RemoveAll(wt.socketDir); err != nil {
			return err
		}

		return callback(c)
	}()
}

// Close cleans up sidechannel resources. If the callback is already
// running, Close will block until the callback is done.
func (wt *SidechannelWaiter) Close() error {
	// Run all cleanup actions _before_ checking errors, so that we cannot
	// forget one.
	cleanupErrors := []error{
		// If wt.run() is blocked on AcceptUnix(), this will unblock it.
		wt.listener.Close(),
		// Remove the socket directory to prevent garbage in case wt.run() did
		// not run.
		os.RemoveAll(wt.socketDir),
		// Block until wt.run() is done.
		wt.Wait(),
	}

	for _, err := range cleanupErrors {
		if err != nil {
			return err
		}
	}

	return nil
}

// Wait waits for the callback to run and returns its error value.
func (wt *SidechannelWaiter) Wait() error { return <-wt.errC }
