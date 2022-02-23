package hook

import (
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	grpc_metadata "google.golang.org/grpc/metadata"
)

func TestSidechannel(t *testing.T) {
	ctx := testhelper.Context(t)

	// Client side
	ctxOut, wt, err := SetupSidechannel(
		ctx,
		func(c *net.UnixConn) error {
			_, err := io.WriteString(c, "ping")
			return err
		},
	)
	require.NoError(t, err)
	defer wt.Close()

	require.DirExists(t, wt.socketDir)

	// Server side
	ctxIn := metadata.OutgoingToIncoming(ctxOut)
	c, err := GetSidechannel(ctxIn)
	require.NoError(t, err)
	defer c.Close()

	buf, err := io.ReadAll(c)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))

	require.NoDirExists(t, wt.socketDir)

	// Client side
	require.NoError(t, wt.Wait())
}

func TestSidechannel_cleanup(t *testing.T) {
	_, wt, err := SetupSidechannel(
		testhelper.Context(t),
		func(c *net.UnixConn) error { return nil },
	)
	require.NoError(t, err)
	defer wt.Close()

	require.DirExists(t, wt.socketDir)
	_ = wt.Close()
	require.NoDirExists(t, wt.socketDir)
}

func TestGetSidechannel(t *testing.T) {
	ctx := testhelper.Context(t)

	testCases := []string{
		"foobar",
		"sc.foo/../../bar",
		"foo/../../bar",
		"/etc/passwd",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ctx := grpc_metadata.NewIncomingContext(
				ctx,
				map[string][]string{sidechannelHeader: {tc}},
			)
			_, err := GetSidechannel(ctx)
			require.Error(t, err)
			require.Equal(t, &errInvalidSidechannelAddress{tc}, err)
		})
	}
}
