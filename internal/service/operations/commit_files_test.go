	"context"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
func testSuccessfulUserCommitFilesRequest(t *testing.T, ctxWithFeatureFlags context.Context) {
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
			ctx := metadata.NewOutgoingContext(ctxWithFeatureFlags, md)
			headCommit, err := log.GetCommit(ctxWithFeatureFlags, tc.repo, tc.branchName)
func TestSuccessfulUserCommitFilesRequest(t *testing.T) {
	featureSet, err := testhelper.NewFeatureSets(nil, featureflag.GitalyRubyCallHookRPC, featureflag.GoUpdateHook)
	require.NoError(t, err)
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, features := range featureSet {
		t.Run(features.String(), func(t *testing.T) {
			ctx = features.WithParent(ctx)
			testSuccessfulUserCommitFilesRequest(t, ctx)
		})
	}
}

	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()
	serverSocketPath, stop := operations.RunOperationServiceServer(t)
	defer stop()