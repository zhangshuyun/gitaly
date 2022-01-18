package stream

import (
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
)

// Conn represents a bi-directional client side connection.
type Conn interface {
	io.Reader
	io.Writer
	CloseWrite() error
}

// BandStdout and BandStderr are the pktline band ID's for stdout and
// stderr, respectively.
const (
	BandStdout = 1
	BandStderr = 2
)

// ProxyPktLine lets a client de-multiplex and proxy stdin, stdout and
// stderr streams. Stdout and stderr are interleaved with Git's pktline
// format.
func ProxyPktLine(c Conn, stdin io.Reader, stdout, stderr io.Writer) error {
	copyRequest := func(c Conn) error {
		if _, err := io.Copy(c, stdin); err != nil {
			return fmt.Errorf("copy request: %w", err)
		}
		if err := c.CloseWrite(); err != nil {
			return fmt.Errorf("close request: %w", err)
		}
		return nil
	}

	copyResponse := func(c Conn) error {
		if err := pktline.EachSidebandPacket(c, func(band byte, data []byte) (err error) {
			switch band {
			case BandStdout:
				_, err = stdout.Write(data)
			case BandStderr:
				_, err = stderr.Write(data)
			default:
				err = fmt.Errorf("unexpected band: %d", band)
			}
			return err
		}); err != nil {
			return fmt.Errorf("copy response: %w", err)
		}

		return nil
	}

	request := make(chan error, 1)
	go func() { request <- copyRequest(c) }()

	response := make(chan error, 1)
	go func() { response <- copyResponse(c) }()

	for {
		select {
		case err := <-response:
			// Server is done. No point in waiting for client.
			return err
		case err := <-request:
			if err != nil {
				return err
			}
			// Client is now done. Wait for server to finish too.
		}
	}
}
