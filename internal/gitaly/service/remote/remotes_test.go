package remote

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulAddRemote(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		description           string
		remoteName            string
		url                   string
		mirrorRefmaps         []string
		resolvedMirrorRefmaps []string
	}{
		{
			description: "creates a new remote",
			remoteName:  "my-remote",
			url:         "http://my-repo.git",
		},
		{
			description: "if a remote with the same name exists, it updates it",
			remoteName:  "my-remote",
			url:         "johndoe@host:my-new-repo.git",
		},
		{
			description:   "doesn't set the remote as mirror if mirror_refmaps is not `present`",
			remoteName:    "my-non-mirror-remote",
			url:           "johndoe@host:my-new-repo.git",
			mirrorRefmaps: []string{""},
		},
		{
			description:           "sets the remote as mirror if a mirror_refmap is present",
			remoteName:            "my-mirror-remote",
			url:                   "http://my-mirror-repo.git",
			mirrorRefmaps:         []string{"all_refs"},
			resolvedMirrorRefmaps: []string{"+refs/*:refs/*"},
		},
		{
			description:           "sets the remote as mirror with multiple mirror_refmaps",
			remoteName:            "my-other-mirror-remote",
			url:                   "http://my-non-mirror-repo.git",
			mirrorRefmaps:         []string{"all_refs", "+refs/pull/*/head:refs/merge-requests/*/head"},
			resolvedMirrorRefmaps: []string{"+refs/*:refs/*", "+refs/pull/*/head:refs/merge-requests/*/head"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			request := &gitalypb.AddRemoteRequest{
				Repository:    testRepo,
				Name:          tc.remoteName,
				Url:           tc.url,
				MirrorRefmaps: tc.mirrorRefmaps,
			}

			_, err := client.AddRemote(ctx, request)
			require.NoError(t, err)

			remotes := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "-v")

			require.Contains(t, string(remotes), fmt.Sprintf("%s\t%s (fetch)", tc.remoteName, tc.url))
			require.Contains(t, string(remotes), fmt.Sprintf("%s\t%s (push)", tc.remoteName, tc.url))

			mirrorConfigRegexp := fmt.Sprintf("remote.%s", tc.remoteName)
			mirrorConfig := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "--get-regexp", mirrorConfigRegexp))
			if len(tc.resolvedMirrorRefmaps) > 0 {
				for _, resolvedMirrorRefmap := range tc.resolvedMirrorRefmaps {
					require.Contains(t, mirrorConfig, resolvedMirrorRefmap)
				}
				require.Contains(t, mirrorConfig, "mirror true")
				require.Contains(t, mirrorConfig, "prune true")
			} else {
				require.NotContains(t, mirrorConfig, "mirror true")
			}
		})
	}
}

func TestFailedAddRemoteDueToValidation(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		description string
		remoteName  string
		url         string
	}{
		{
			description: "Remote name empty",
			url:         "http://my-repo.git",
		},
		{
			description: "Remote name blank",
			remoteName:  "    ",
			url:         "http://my-repo.git",
		},
		{
			description: "URL empty",
			remoteName:  "my-remote",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			request := &gitalypb.AddRemoteRequest{
				Repository: testRepo,
				Name:       tc.remoteName,
				Url:        tc.url,
			}

			_, err := client.AddRemote(ctx, request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulRemoveRemote(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "my-remote", "http://my-repo.git")

	testCases := []struct {
		description string
		remoteName  string
		result      bool
	}{
		{
			description: "removes the remote",
			remoteName:  "my-remote",
			result:      true,
		},
		{
			description: "returns false if the remote doesn't exist",
			remoteName:  "not-a-real-remote",
			result:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			request := &gitalypb.RemoveRemoteRequest{
				Repository: testRepo,
				Name:       tc.remoteName,
			}

			r, err := client.RemoveRemote(ctx, request)
			require.NoError(t, err)
			require.Equal(t, tc.result, r.GetResult())

			remotes := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote")

			require.NotContains(t, string(remotes), tc.remoteName)
		})
	}
}

func TestFailedRemoveRemoteDueToValidation(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.RemoveRemoteRequest{Repository: testRepo} // Remote name empty

	_, err := client.RemoveRemote(ctx, request)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestFindRemoteRepository(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoRefs := testhelper.MustReadFile(t, "testdata/lsremotedata.txt")
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		io.Copy(w, bytes.NewReader(infoRefs))
	}))
	defer ts.Close()

	resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: ts.URL, StorageName: "default"})
	require.NoError(t, err)

	require.True(t, resp.Exists)
}

func TestFailedFindRemoteRepository(t *testing.T) {
	serverSocketPath := runRemoteServiceServer(t, config.Config)

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

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
		resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: tc.remote, StorageName: "default"})
		if tc.code == codes.OK {
			require.NoError(t, err)
		} else {
			testhelper.RequireGrpcError(t, err, tc.code)
			continue
		}

		require.Equal(t, tc.exists, resp.GetExists(), tc.description)
	}
}
