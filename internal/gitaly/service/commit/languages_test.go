package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestLanguages(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	request := &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("cb19058ecc02d01f8e4290b7e79cafd16a8839b6"),
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	require.NotZero(t, len(resp.Languages), "number of languages in response")

	expectedLanguages := []gitalypb.CommitLanguagesResponse_Language{
		{Name: "Ruby", Share: 66, Color: "#701516", FileCount: 4, Bytes: 2943},
		{Name: "JavaScript", Share: 22, Color: "#f1e05a", FileCount: 1, Bytes: 1014},
		{Name: "HTML", Share: 7, Color: "#e34c26", FileCount: 1, Bytes: 349},
		{Name: "CoffeeScript", Share: 2, Color: "#244776", FileCount: 1, Bytes: 107},
		// Modula-2 is a special case because Linguist has no color for it. This
		// test case asserts that we invent a color for it (SHA256 of the name).
		{Name: "Modula-2", Share: 2, Color: "#3fd5e0", FileCount: 1, Bytes: 95},
	}

	require.Equal(t, len(expectedLanguages), len(resp.Languages))

	for i, el := range expectedLanguages {
		actualLanguage := resp.Languages[i]
		requireLanguageEqual(t, &el, actualLanguage)
	}
}

func TestFileCountIsZeroWhenFeatureIsDisabled(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	request := &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("cb19058ecc02d01f8e4290b7e79cafd16a8839b6"),
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	resp, err := client.CommitLanguages(ctx, request)
	require.NoError(t, err)

	require.NotZero(t, len(resp.Languages), "number of languages in response")

	for i := range resp.Languages {
		actualLanguage := resp.Languages[i]
		require.Equal(t, uint32(0), actualLanguage.FileCount)
	}
}

func requireLanguageEqual(t *testing.T, expected, actual *gitalypb.CommitLanguagesResponse_Language) {
	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, expected.Color, actual.Color)
	require.False(t, (expected.Share-actual.Share)*(expected.Share-actual.Share) >= 1.0, "shares do not match")
	require.Equal(t, expected.Bytes, actual.Bytes)
}

func TestLanguagesEmptyRevision(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	request := &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
	}

	ctx, cancel := testhelper.Context()
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

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.CommitLanguages(ctx, &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("--output=/meow"),
	})
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestAmbiguousRefCommitLanguagesRequestRevision(t *testing.T) {
	server, serverSocketPath := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	// gitlab-test repo has both a branch and a tag named 'v1.1.0'
	// b83d6e391c22777fca1ed3012fce84f633d7fed0 refs/heads/v1.1.0
	// 8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0
	_, err := client.CommitLanguages(ctx, &gitalypb.CommitLanguagesRequest{
		Repository: testRepo,
		Revision:   []byte("v1.1.0"),
	})
	require.NoError(t, err)
}
