package stats

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestClone(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repoPath, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	serverPort, stopGitServer := gittest.GitServer(t, config.Config, repoPath, nil)
	defer stopGitServer()

	clone := Clone{URL: fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath))}
	require.NoError(t, clone.Perform(ctx), "perform analysis clone")

	const expectedWants = 90 // based on contents of _support/gitlab-test.git-packed-refs
	require.Greater(t, clone.RefsWanted(), expectedWants, "number of wanted refs")

	require.Equal(t, 200, clone.Get.HTTPStatus(), "get status")
	require.Greater(t, clone.Get.Packets, 0, "number of get packets")
	require.Greater(t, clone.Get.PayloadSize, int64(0), "get payload size")
	require.Greater(t, len(clone.Get.Caps), 10, "get capabilities")

	previousValue := time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.Get.ResponseHeader()},
		{"time to first packet", clone.Get.FirstGitPacket()},
		{"time to receive response body", clone.Get.ResponseBody()},
	} {
		require.True(t, m.value > previousValue, "get: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}

	require.Equal(t, 200, clone.Post.HTTPStatus(), "post status")
	require.Greater(t, clone.Post.Packets(), 0, "number of post packets")

	require.Greater(t, clone.Post.BandPackets("progress"), 0, "number of progress packets")
	require.Greater(t, clone.Post.BandPackets("pack"), 0, "number of pack packets")

	require.Greater(t, clone.Post.BandPayloadSize("progress"), int64(0), "progress payload bytes")
	require.Greater(t, clone.Post.BandPayloadSize("pack"), int64(0), "pack payload bytes")

	previousValue = time.Duration(0)
	for _, m := range []struct {
		desc  string
		value time.Duration
	}{
		{"time to receive response header", clone.Post.ResponseHeader()},
		{"time to receive NAK", clone.Post.NAK()},
		{"time to receive first progress message", clone.Post.BandFirstPacket("progress")},
		{"time to receive first pack message", clone.Post.BandFirstPacket("pack")},
		{"time to receive response body", clone.Post.ResponseBody()},
	} {
		require.True(t, m.value > previousValue, "post: expect %s (%v) to be greater than previous value %v", m.desc, m.value, previousValue)
		previousValue = m.value
	}
}

func TestCloneWithAuth(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repoPath, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	const (
		user     = "test-user"
		password = "test-password"
	)

	authWasChecked := false

	serverPort, stopGitServer := gittest.GitServer(t, config.Config, repoPath, func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		authWasChecked = true

		actualUser, actualPassword, ok := r.BasicAuth()
		require.True(t, ok, "request should have basic auth")
		require.Equal(t, user, actualUser)
		require.Equal(t, password, actualPassword)

		next.ServeHTTP(w, r)
	})
	defer stopGitServer()

	clone := Clone{
		URL:      fmt.Sprintf("http://localhost:%d/%s", serverPort, filepath.Base(repoPath)),
		User:     user,
		Password: password,
	}
	require.NoError(t, clone.Perform(ctx), "perform analysis clone")

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
