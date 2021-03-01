package remote

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUpdateRemoteMirrorRequest(t *testing.T) {
	serverSocketPath, stop := RunRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepo(t)
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_1"

	testhelper.CreateTag(t, mirrorPath, "v0.0.1", "master", nil) // I needed another tag for the tests
	testhelper.CreateTag(t, testRepoPath, "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98", nil)
	testhelper.CreateTag(t, testRepoPath, "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9", &testhelper.CreateTagOpts{
		Message: "Overriding tag", Force: true})

	// Create a commit that only exists in the mirror
	mirrorOnlyCommitOid := gittest.CreateCommit(t, mirrorPath, "master", nil)
	require.NotEmpty(t, mirrorOnlyCommitOid)

	setupCommands := [][]string{
		// Preconditions
		{"config", "user.email", "gitalytest@example.com"},
		{"remote", "add", remoteName, mirrorPath},
		// Updates
		{"branch", "new-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},                  // Add branch
		{"branch", "ignored-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},              // Add branch not matching branch list
		{"update-ref", "refs/heads/empty-branch", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update branch
		{"branch", "-D", "not-merged-branch"},                                                 // Delete branch

		// Catch bug https://gitlab.com/gitlab-org/gitaly/issues/1421 (reliance
		// on 'HEAD' as the default branch). By making HEAD point to something
		// invalid, we ensure this gets handled correctly.
		{"symbolic-ref", "HEAD", "refs/does/not/exist"},
		{"tag", "--delete", "v1.1.0"}, // v1.1.0 is ambiguous, maps to a branch and a tag in gitlab-test repository
	}

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		testhelper.MustRunCommand(t, nil, "git", gitArgs...)
	}

	newTagOid := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "v1.0.0"))
	newTagOid = strings.TrimSpace(newTagOid)
	require.NotEqual(t, newTagOid, "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8") // Sanity check that the tag did in fact change

	firstRequest := &gitalypb.UpdateRemoteMirrorRequest{
		Repository:           testRepo,
		RefName:              remoteName,
		OnlyBranchesMatching: nil,
	}
	matchingRequest1 := &gitalypb.UpdateRemoteMirrorRequest{
		OnlyBranchesMatching: [][]byte{[]byte("new-branch"), []byte("empty-branch")},
	}
	matchingRequest2 := &gitalypb.UpdateRemoteMirrorRequest{
		OnlyBranchesMatching: [][]byte{[]byte("not-merged-branch"), []byte("matcher-without-matches")},
	}

	stream, err := client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))
	require.NoError(t, stream.Send(matchingRequest1))
	require.NoError(t, stream.Send(matchingRequest2))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, response.DivergentRefs)

	// Ensure the local repository still has no reference to the mirror-only commit
	localRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref"))
	require.NotContains(t, localRefs, mirrorOnlyCommitOid)

	mirrorRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))

	require.Contains(t, mirrorRefs, mirrorOnlyCommitOid)
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/new-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/ignored-branch")
	require.Contains(t, mirrorRefs, "0b4bc9a49b562e85de7cc9e834518ea6828729b9 commit\trefs/heads/empty-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/not-merged-branch")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/tags/new-tag")
	require.Contains(t, mirrorRefs, newTagOid+" tag\trefs/tags/v1.0.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v0.0.1")
	require.Contains(t, mirrorRefs, "refs/heads/v1.1.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v1.1.0")
}

func TestSuccessfulUpdateRemoteMirrorRequestWithWildcards(t *testing.T) {
	serverSocketPath, stop := RunRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepo(t)
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_2"

	setupCommands := [][]string{
		// Preconditions
		{"config", "user.email", "gitalytest@example.com"},
		{"remote", "add", remoteName, mirrorPath},
		// Updates
		{"branch", "11-0-stable", "60ecb67744cb56576c30214ff52294f8ce2def98"},
		{"branch", "11-1-stable", "60ecb67744cb56576c30214ff52294f8ce2def98"},                // Add branch
		{"branch", "ignored-branch", "60ecb67744cb56576c30214ff52294f8ce2def98"},             // Add branch not matching branch list
		{"update-ref", "refs/heads/some-branch", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"}, // Update branch
		{"update-ref", "refs/heads/feature", "0b4bc9a49b562e85de7cc9e834518ea6828729b9"},     // Update branch
		// Scoped to the project, so will be removed after
		{"branch", "-D", "not-merged-branch"}, // Delete branch
		{"tag", "--delete", "v1.1.0"},         // v1.1.0 is ambiguous, maps to a branch and a tag in gitlab-test repository
	}

	testhelper.CreateTag(t, testRepoPath, "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98", nil) // Add tag
	testhelper.CreateTag(t, testRepoPath, "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		&testhelper.CreateTagOpts{Message: "Overriding tag", Force: true}) // Update tag

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		testhelper.MustRunCommand(t, nil, "git", gitArgs...)
	}

	// Workaround for https://gitlab.com/gitlab-org/gitaly/issues/1439
	// Create a tag on the remote to ensure it gets deleted later
	testhelper.CreateTag(t, mirrorPath, "v1.2.0", "master", nil)

	newTagOid := string(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "v1.0.0"))
	newTagOid = strings.TrimSpace(newTagOid)
	require.NotEqual(t, newTagOid, "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8") // Sanity check that the tag did in fact change
	firstRequest := &gitalypb.UpdateRemoteMirrorRequest{
		Repository:           testRepo,
		RefName:              remoteName,
		OnlyBranchesMatching: [][]byte{[]byte("*-stable"), []byte("feature")},
	}

	stream, err := client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, response.DivergentRefs)

	mirrorRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/11-0-stable")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/heads/11-1-stable")
	require.Contains(t, mirrorRefs, "0b4bc9a49b562e85de7cc9e834518ea6828729b9 commit\trefs/heads/feature")
	require.NotContains(t, mirrorRefs, "refs/heads/ignored-branch")
	require.NotContains(t, mirrorRefs, "refs/heads/some-branch")
	require.Contains(t, mirrorRefs, "refs/heads/not-merged-branch")
	require.Contains(t, mirrorRefs, "60ecb67744cb56576c30214ff52294f8ce2def98 commit\trefs/tags/new-tag")
	require.Contains(t, mirrorRefs, newTagOid+" tag\trefs/tags/v1.0.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v1.2.0")
	require.Contains(t, mirrorRefs, "refs/heads/v1.1.0")
	require.NotContains(t, mirrorRefs, "refs/tags/v1.1.0")
}

func TestSuccessfulUpdateRemoteMirrorRequestWithKeepDivergentRefs(t *testing.T) {
	serverSocketPath, stop := RunRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepo(t)
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_1"

	testhelper.CreateTag(t, mirrorPath, "v2.0.0", "master", nil)

	setupCommands := [][]string{
		// Preconditions
		{"config", "user.email", "gitalytest@example.com"},
		{"remote", "add", remoteName, mirrorPath},

		// Create a divergence by moving `master` to the HEAD of another branch
		// ba3faa7d only exists on `after-create-delete-modify-move`
		{"update-ref", "refs/heads/master", "ba3faa7dbecdb555c748b36e8bc0f427e69de5e7"},

		// Delete a branch to ensure it's kept around in the mirror
		{"branch", "-D", "not-merged-branch"},
	}

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		testhelper.MustRunCommand(t, nil, "git", gitArgs...)
	}
	firstRequest := &gitalypb.UpdateRemoteMirrorRequest{
		Repository:        testRepo,
		RefName:           remoteName,
		KeepDivergentRefs: true,
	}

	stream, err := client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.ElementsMatch(t, response.DivergentRefs, [][]byte{[]byte("refs/heads/master")})

	mirrorRefs := string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))

	// Verify `master` didn't get updated, since its HEAD is no longer an ancestor of remote's version
	require.Contains(t, mirrorRefs, "1e292f8fedd741b75372e19097c76d327140c312 commit\trefs/heads/master")

	// Verify refs missing on the source stick around on the mirror
	require.Contains(t, mirrorRefs, "refs/heads/not-merged-branch")
	require.Contains(t, mirrorRefs, "refs/tags/v2.0.0")

	// Re-run mirroring without KeepDivergentRefs
	firstRequest.KeepDivergentRefs = false

	stream, err = client.UpdateRemoteMirror(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstRequest))

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	mirrorRefs = string(testhelper.MustRunCommand(t, nil, "git", "-C", mirrorPath, "for-each-ref"))

	// Verify `master` gets overwritten with the value from the source
	require.Contains(t, mirrorRefs, "ba3faa7dbecdb555c748b36e8bc0f427e69de5e7 commit\trefs/heads/master")

	// Verify a branch only on the mirror is now deleted
	require.NotContains(t, mirrorRefs, "refs/heads/not-merged-branch")
}

func TestFailedUpdateRemoteMirrorRequestDueToValidation(t *testing.T) {
	serverSocketPath, stop := RunRemoteServiceServer(t)
	defer stop()

	client, conn := NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc    string
		request *gitalypb.UpdateRemoteMirrorRequest
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: nil,
				RefName:    "remote_mirror_1",
			},
		},
		{
			desc: "empty RefName",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: testRepo,
				RefName:    "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.UpdateRemoteMirror(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(tc.request))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}
