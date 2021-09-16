package hook

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"google.golang.org/grpc/metadata"
)

func TestSidechannel(t *testing.T) {
	// Client side
	ctxOut, wt, err := SetupSidechannel(
		context.Background(),
		func(c *net.UnixConn) error {
			_, err := io.WriteString(c, "ping")
			return err
		},
	)
	require.NoError(t, err)
	defer wt.Close()

	// Server side
	ctxIn := helper.OutgoingToIncoming(ctxOut)
	c, err := GetSidechannel(ctxIn)
	require.NoError(t, err)
	defer c.Close()

	buf, err := io.ReadAll(c)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))

	// Client side
	require.NoError(t, wt.Wait())
}

func TestGetSidechannel(t *testing.T) {
	testCases := []string{
		"foobar",
		"sc.foo/../../bar",
		"foo/../../bar",
		"/etc/passwd",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(
				context.Background(),
				map[string][]string{sidechannelHeader: {tc}},
			)
			_, err := GetSidechannel(ctx)
			require.Error(t, err)
			require.Equal(t, &errInvalidSidechannelAddress{tc}, err)
		})
	}
}
