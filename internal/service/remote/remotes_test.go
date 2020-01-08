package remote

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulAddRemote(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := context.WithCancel(context.Background())
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
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := context.WithCancel(context.Background())
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
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := context.WithCancel(context.Background())
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
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request := &gitalypb.RemoveRemoteRequest{Repository: testRepo} // Remote name empty

	_, err := client.RemoveRemote(ctx, request)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestFindRemoteRepository(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoRefs := testhelper.MustReadFile(t, "testdata/lsremotedata.txt")
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		io.Copy(w, bytes.NewReader(infoRefs))
	}))
	defer ts.Close()

	resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: ts.URL})
	require.NoError(t, err)

	require.True(t, resp.Exists)
}

func TestFailedFindRemoteRepository(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
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
		resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: tc.remote})
		if tc.code == codes.OK {
			require.NoError(t, err)
		} else {
			testhelper.RequireGrpcError(t, err, tc.code)
			continue
		}

		require.Equal(t, tc.exists, resp.GetExists(), tc.description)
	}
}

func TestListDifferentPushUrlRemote(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client.RemoveRemote(ctx, &gitalypb.RemoveRemoteRequest{
		Repository: testRepo,
		Name:       "origin",
	})

	branchName := "my-remote"
	fetchURL := "http://my-repo.git"
	pushURL := "http://my-other-repo.git"

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", branchName, fetchURL)
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "set-url", "--push", branchName, pushURL)

	testCases := []*gitalypb.ListRemotesResponse_Remote{
		{
			Name:     branchName,
			FetchUrl: fetchURL,
			PushUrl:  pushURL,
		},
	}

	request := &gitalypb.ListRemotesRequest{Repository: testRepo}

	resp, err := client.ListRemotes(ctx, request)
	if err != nil {
		t.Fatal(err)
	}

	receivedRemotes := consumeListRemotesResponse(t, resp)
	require.NoError(t, err)

	require.Len(t, receivedRemotes, len(testCases))
	require.ElementsMatch(t, testCases, receivedRemotes)
}

func TestListRemotes(t *testing.T) {
	stop, serverSocketPath := runRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repoWithSingleRemote, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoWithMultipleRemotes, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoWithEmptyRemote, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	singleRemote := []*gitalypb.ListRemotesResponse_Remote{
		{Name: "my-remote", FetchUrl: "http://my-repo.git", PushUrl: "http://my-repo.git"},
	}

	multipleRemotes := []*gitalypb.ListRemotesResponse_Remote{
		{Name: "my-other-remote", FetchUrl: "johndoe@host:my-new-repo.git", PushUrl: "johndoe@host:my-new-repo.git"},
		{Name: "my-remote", FetchUrl: "http://my-repo.git", PushUrl: "http://my-repo.git"},
	}

	testCases := []struct {
		description string
		repository  *gitalypb.Repository
		remotes     []*gitalypb.ListRemotesResponse_Remote
	}{
		{
			description: "empty remote",
			repository:  repoWithEmptyRemote,
			remotes:     []*gitalypb.ListRemotesResponse_Remote{},
		},
		{
			description: "single remote",
			repository:  repoWithSingleRemote,
			remotes:     singleRemote,
		},
		{
			description: "multiple remotes",
			repository:  repoWithMultipleRemotes,
			remotes:     multipleRemotes,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			client.RemoveRemote(ctx, &gitalypb.RemoveRemoteRequest{
				Repository: tc.repository,
				Name:       "origin",
			})

			for _, r := range tc.remotes {
				request := &gitalypb.AddRemoteRequest{
					Repository: tc.repository,
					Name:       r.Name,
					Url:        r.FetchUrl,
				}

				_, err := client.AddRemote(ctx, request)
				require.NoError(t, err)
			}

			request := &gitalypb.ListRemotesRequest{Repository: tc.repository}

			resp, err := client.ListRemotes(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			receivedRemotes := consumeListRemotesResponse(t, resp)
			require.NoError(t, err)

			require.Len(t, receivedRemotes, len(tc.remotes))
			require.ElementsMatch(t, tc.remotes, receivedRemotes)
		})
	}
}

func consumeListRemotesResponse(t *testing.T, l gitalypb.RemoteService_ListRemotesClient) []*gitalypb.ListRemotesResponse_Remote {
	receivedRemotes := []*gitalypb.ListRemotesResponse_Remote{}
	for {
		resp, err := l.Recv()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		receivedRemotes = append(receivedRemotes, resp.GetRemotes()...)
	}

	return receivedRemotes
}
