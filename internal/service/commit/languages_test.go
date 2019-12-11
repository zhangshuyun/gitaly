package commit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

	ctx = EnableLinguistFileCountStatsFeatureFlag(ctx)

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

	// if another request is made within short succession (1 week), the second
	// one is rejected
	_, err = client.CommitLanguages(ctx, request)
	require.Error(t, err)
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
}

func TestFileCountIsZeroWhenFeatureIsDisabled(t *testing.T) {
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

	for i := range resp.Languages {
		actualLanguage := resp.Languages[i]
		require.Equal(t, uint32(0), actualLanguage.FileCount)
	}
}

func requireLanguageEqual(t *testing.T, expected, actual *gitalypb.CommitLanguagesResponse_Language) {
	require.Equal(t, expected.Name, actual.Name)
	require.Equal(t, expected.Color, actual.Color)
	require.False(t, (expected.Share-actual.Share)*(expected.Share-actual.Share) >= 1.0, "shares do not match")
	require.Equal(t, expected.FileCount, actual.FileCount)
	require.Equal(t, expected.Bytes, actual.Bytes)
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

func EnableLinguistFileCountStatsFeatureFlag(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		featureflag.HeaderKey(featureflag.LinguistFileCountStats): "true",
	}))
}
