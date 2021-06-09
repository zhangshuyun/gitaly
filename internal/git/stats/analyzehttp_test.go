package stats

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestClone(t *testing.T) {
	cfg, _, repoPath := testcfg.BuildWithRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	serverPort, stopGitServer := gittest.GitServer(t, cfg, repoPath, nil)
	defer func() {
		require.NoError(t, stopGitServer())
	}()

	clone, err := PerformClone(ctx, fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath)), "", "", false)
	require.NoError(t, err, "perform analysis clone")

	const expectedRequests = 90 // based on contents of _support/gitlab-test.git-packed-refs
	require.Greater(t, clone.FetchPack.RefsWanted(), expectedRequests, "number of wanted refs")

	require.Equal(t, 200, clone.ReferenceDiscovery.HTTPStatus(), "get status")
	require.Greater(t, clone.ReferenceDiscovery.Packets(), 0, "number of get packets")
	require.Greater(t, clone.ReferenceDiscovery.PayloadSize(), int64(0), "get payload size")
	require.Greater(t, len(clone.ReferenceDiscovery.Caps()), 10, "get capabilities")

	previousValue := time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.ReferenceDiscovery.ResponseHeader()},
		{"time to first packet", clone.ReferenceDiscovery.FirstGitPacket()},
		{"time to receive response body", clone.ReferenceDiscovery.ResponseBody()},
	} {
		require.True(t, m.value > previousValue, "get: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}

	require.Equal(t, 200, clone.FetchPack.HTTPStatus(), "post status")
	require.Greater(t, clone.FetchPack.Packets(), 0, "number of post packets")

	require.Greater(t, clone.FetchPack.BandPackets("progress"), 0, "number of progress packets")
	require.Greater(t, clone.FetchPack.BandPackets("pack"), 0, "number of pack packets")

	require.Greater(t, clone.FetchPack.BandPayloadSize("progress"), int64(0), "progress payload bytes")
	require.Greater(t, clone.FetchPack.BandPayloadSize("pack"), int64(0), "pack payload bytes")

	previousValue = time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.FetchPack.ResponseHeader()},
		{"time to receive NAK", clone.FetchPack.NAK()},
		{"time to receive first progress message", clone.FetchPack.BandFirstPacket("progress")},
		{"time to receive first pack message", clone.FetchPack.BandFirstPacket("pack")},
		{"time to receive response body", clone.FetchPack.ResponseBody()},
	} {
		require.True(t, m.value > previousValue, "post: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}
}

func TestCloneWithAuth(t *testing.T) {
	cfg, _, repoPath := testcfg.BuildWithRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		user     = "test-user"
		password = "test-password"
	)

	authWasChecked := false

	serverPort, stopGitServer := gittest.GitServer(t, cfg, repoPath, func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		authWasChecked = true

		actualUser, actualPassword, ok := r.BasicAuth()
		require.True(t, ok, "request should have basic auth")
		require.Equal(t, user, actualUser)
		require.Equal(t, password, actualPassword)

		next.ServeHTTP(w, r)
	})
	defer func() {
		require.NoError(t, stopGitServer())
	}()

	_, err := PerformClone(
		ctx,
		fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath)),
		user,
		password,
		false,
	)
	require.NoError(t, err, "perform analysis clone")

	require.True(t, authWasChecked, "authentication middleware should have gotten triggered")
}

func TestBandToHuman(t *testing.T) {
	testCases := []struct {
		in   byte
		out  string
		fail bool
	}{
		{in: 0, fail: true},
		{in: 1, out: "pack"},
		{in: 2, out: "progress"},
		{in: 3, out: "error"},
		{in: 4, fail: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("band index %d", tc.in), func(t *testing.T) {
			out, err := bandToHuman(tc.in)

			if tc.fail {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.out, out, "band name")
		})
	}
}
