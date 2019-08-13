package commit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLanguages(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("cb19058ecc02d01f8e4290b7e79cafd16a8839b6"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	require.NotZero(t, len(resp.Languages), "number of languages in response")

	expectedLanguages := []gitalypb.CommitLanguagesResponse_Language{
		{Name: "Ruby", Share: 66, Color: "#701516"},
		{Name: "JavaScript", Share: 22, Color: "#f1e05a"},
		{Name: "HTML", Share: 7, Color: "#e34c26"},
		{Name: "CoffeeScript", Share: 2, Color: "#244776"},
		// Modula-2 is a special case because Linguist has no color for it. This
		// test case asserts that we invent a color for it (SHA256 of the name).
		{Name: "Modula-2", Share: 2, Color: "#3fd5e0"},
	}

	require.Equal(t, len(expectedLanguages), len(resp.Languages))

	for i, el := range expectedLanguages {
		actualLanguage := resp.Languages[i]
		require.True(t, languageEqual(&el, actualLanguage), "expected %+v, got %+v", el, *actualLanguage)
	}
}

func languageEqual(expected, actual *gitalypb.CommitLanguagesResponse_Language) bool {
	if expected.Name != actual.Name {
		return false
	}
	if expected.Color != actual.Color {
		return false
	}
	if (expected.Share-actual.Share)*(expected.Share-actual.Share) >= 1.0 {
		return false
	}
	return true
}

func TestLanguagesEmptyRevision(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	require.NotZero(t, len(resp.Languages), "number of languages in response")

	foundRuby := false
	for _, l := range resp.Languages {
		if l.Name == "Ruby" {
			foundRuby = true
		}
	}
	require.True(t, foundRuby, "expected to find Ruby as a language on HEAD")
}

func TestInvalidCommitLanguagesRequestRevision(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.CommitLanguages(ctx, &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("--output=/meow"),
	})
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
