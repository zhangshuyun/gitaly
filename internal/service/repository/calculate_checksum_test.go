package repository

import (
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCalculateChecksum(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(path.Join(testRepoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(path.Join(testRepoPath, d), 0755))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-packed-refs", path.Join(testRepoPath, "packed-refs")).Run())
	require.NoError(t, exec.Command("git", "-C", testRepoPath, "symbolic-ref", "HEAD", "refs/heads/feature").Run())

	request := &gitalypb.CalculateChecksumRequest{Repository: testRepo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "0c500d7c8a9dbf65e4cf5e58914bec45bfb6e9ab", response.Checksum)
}

func TestRefWhitelist(t *testing.T) {
	testCases := []struct {
		ref   string
		match bool
	}{
		{ref: "1450cd639e0bc6721eb02800169e464f212cde06 foo/bar", match: false},
		{ref: "259a6fba859cc91c54cd86a2cbd4c2f720e3a19d foo/bar:baz", match: false},
		{ref: "d0a293c0ac821fadfdc086fe528f79423004229d refs/foo/bar:baz", match: false},
		{ref: "21751bf5cb2b556543a11018c1f13b35e44a99d7 tags/v0.0.1", match: false},
		{ref: "498214de67004b1da3d820901307bed2a68a8ef6 keep-around/498214de67004b1da3d820901307bed2a68a8ef6", match: false},
		{ref: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e merge-requests/11/head", match: false},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 environments/3/head", match: false},
		{ref: "4ed78158b5b018c43005cec917129861541e25bc notes/commits", match: false},
		{ref: "9298d46305ee0d3f4ce288370beaa02637584ff2 HEAD", match: true},
		{ref: "0ed8c6c6752e8c6ea63e7b92a517bf5ac1209c80 refs/heads/markdown", match: true},
		{ref: "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0", match: true},
		{ref: "78a30867c755d774340108cdad5f11254818fb0c refs/keep-around/78a30867c755d774340108cdad5f11254818fb0c", match: true},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 refs/merge-requests/10/head", match: true},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 refs/environments/2/head", match: true},
		{ref: "4ed78158b5b018c43005cec917129861541e25bc refs/notes/commits", match: true},
	}

	for _, tc := range testCases {
		t.Run(tc.ref, func(t *testing.T) {
			match := refWhitelist.Match([]byte(tc.ref))
			require.Equal(t, match, tc.match)
		})
	}
}

func TestEmptyRepositoryCalculateChecksum(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "0000000000000000000000000000000000000000", response.Checksum)
}

func TestBrokenRepositoryCalculateChecksum(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	repo, testRepoPath, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	// Force an empty HEAD file
	require.NoError(t, os.Truncate(path.Join(testRepoPath, "HEAD"), 0))

	request := &gitalypb.CalculateChecksumRequest{Repository: repo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	_, err := client.CalculateChecksum(testCtx, request)
	testhelper.RequireGrpcError(t, err, codes.DataLoss)
}

func TestFailedCalculateChecksum(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		desc    string
		request *gitalypb.CalculateChecksumRequest
		code    codes.Code
	}{
		{
			desc:    "Invalid repository",
			request: &gitalypb.CalculateChecksumRequest{Repository: invalidRepo},
			code:    codes.InvalidArgument,
		},
		{
			desc:    "Repository is nil",
			request: &gitalypb.CalculateChecksumRequest{},
			code:    codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		testCtx, cancelCtx := testhelper.Context()
		defer cancelCtx()

		_, err := client.CalculateChecksum(testCtx, testCase.request)
		testhelper.RequireGrpcError(t, err, testCase.code)
	}
}

func TestInvalidRefsCalculateChecksum(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Force the refs database of testRepo into a known state
	require.NoError(t, os.RemoveAll(path.Join(testRepoPath, "refs")))
	for _, d := range []string{"refs/heads", "refs/tags", "refs/notes"} {
		require.NoError(t, os.MkdirAll(path.Join(testRepoPath, d), 0755))
	}
	require.NoError(t, exec.Command("cp", "testdata/checksum-test-invalid-refs", path.Join(testRepoPath, "packed-refs")).Run())

	request := &gitalypb.CalculateChecksumRequest{Repository: testRepo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.CalculateChecksum(testCtx, request)
	require.NoError(t, err)
	require.Equal(t, "0000000000000000000000000000000000000000", response.Checksum)
}
