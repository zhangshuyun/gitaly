	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	featureSet, err := testhelper.NewFeatureSets(nil, featureflag.GoUpdateHook)
	require.NoError(t, err)
	for _, features := range featureSet {
		t.Run(features.String(), func(t *testing.T) {
			ctx = features.WithParent(ctx)
			testSuccessfulUserCommitFilesRequest(t, ctx)
		})
	}