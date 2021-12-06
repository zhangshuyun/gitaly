package remote

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRemoteRepository(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoRefs := testhelper.MustReadFile(t, "testdata/lsremotedata.txt")
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		_, err := io.Copy(w, bytes.NewReader(infoRefs))
		require.NoError(t, err)
	}))
	defer ts.Close()

	resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: ts.URL, StorageName: repo.GetStorageName()})
	require.NoError(t, err)

	require.True(t, resp.Exists)
}

func TestFailedFindRemoteRepository(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		description string
		remote      string
		exists      bool
		code        codes.Code
	}{
		{"non existing remote", "http://example.com/test.git", false, codes.OK},
		{"empty remote", "", false, codes.InvalidArgument},
	}

	for _, tc := range testCases {
		resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: tc.remote, StorageName: repo.GetStorageName()})
		if tc.code == codes.OK {
			require.NoError(t, err)
		} else {
			testhelper.RequireGrpcCode(t, err, tc.code)
			continue
		}

		require.Equal(t, tc.exists, resp.GetExists(), tc.description)
	}
}
