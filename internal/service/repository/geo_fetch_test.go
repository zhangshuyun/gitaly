package repository

import (
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestGeoFetch(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, remoteRepoPath, cleanupRemoteRepo := testhelper.NewTestRepo(t)
	defer cleanupRemoteRepo()

	// add a branch
	branch := "my-cool-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "update-ref", "refs/heads/"+branch, "master")

	forkedRepo, forkRepoPath, forkRepoCleanup := testhelper.NewTestRepo(t)
	defer forkRepoCleanup()

	req := &gitalypb.GeoFetchRequest{
		Repository: forkedRepo,
		GeoRemote: &gitalypb.GeoRemote{
			Url:                     remoteRepoPath,
			Name:                    "geo",
			HttpAuthorizationHeader: "token",
		},
	}

	_, err := client.GeoFetch(ctx, req)
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", branch)
}

func TestGeoFetchValidationError(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	forkedRepo, _, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	testCases := []struct {
		description string
		request     gitalypb.GeoFetchRequest
	}{
		{
			description: "missing repository",
			request: gitalypb.GeoFetchRequest{
				Repository: nil,
				GeoRemote:  &gitalypb.GeoRemote{},
			},
		},
		{
			description: "missing remote url",
			request: gitalypb.GeoFetchRequest{
				Repository: forkedRepo,
				GeoRemote: &gitalypb.GeoRemote{
					Name: "geo",
					Url:  "",
				},
			},
		},
		{
			description: "missing remote url",
			request: gitalypb.GeoFetchRequest{
				Repository: forkedRepo,
				GeoRemote: &gitalypb.GeoRemote{
					Name: "",
					Url:  "some url",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.GeoFetch(ctx, &tc.request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
