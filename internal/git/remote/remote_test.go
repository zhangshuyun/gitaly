package remote

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestAddRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath, err := helper.GetRepoPath(testRepo)
	require.NoError(t, err)

	testCases := []struct {
		description           string
		remoteName            string
		url                   string
		mirrorRefmaps         []string
		resolvedMirrorRefmaps []string
	}{
		{
			description: "creates remote with no refmaps",
			remoteName:  "no-refmap",
			url:         "https://gitlab.com/gitlab-org/git.git",
		},
		{
			description: "updates url when remote exists",
			remoteName:  "no-refmap",
			url:         "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git",
		},
		{
			description:           "resolves abbreviated refmap",
			remoteName:            "abbrev-refmap",
			url:                   "https://gitlab.com/gitlab-org/git.git",
			mirrorRefmaps:         []string{"all_refs", "heads", "tags"},
			resolvedMirrorRefmaps: []string{"+refs/*:refs/*", "+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"},
		},
		{
			description:           "adds multiple refmaps",
			remoteName:            "multiple-refmaps",
			url:                   "https://gitlab.com/gitlab-org/git.git",
			mirrorRefmaps:         []string{"tags", "+refs/heads/*:refs/remotes/origin/*"},
			resolvedMirrorRefmaps: []string{"+refs/tags/*:refs/tags/*", "+refs/heads/*:refs/remotes/origin/*"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := Add(ctx, testRepo, tc.remoteName, tc.url, tc.mirrorRefmaps)
			require.NoError(t, err)

			found, err := Exists(ctx, testRepo, tc.remoteName)
			require.NoError(t, err)
			require.True(t, found)

			url := string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote", "get-url", tc.remoteName))
			require.Contains(t, url, tc.url)

			mirrorRegex := "remote." + tc.remoteName
			mirrorConfig := string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--get-regexp", mirrorRegex))
			if len(tc.resolvedMirrorRefmaps) > 0 {
				require.Contains(t, mirrorConfig, "mirror true")
				require.Contains(t, mirrorConfig, "prune true")
			} else {
				require.NotContains(t, mirrorConfig, "mirror true")
			}

			mirrorFetch := mirrorRegex + ".fetch"
			fetch := string(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "--get-all", mirrorFetch))
			for _, resolvedMirrorRefmap := range tc.resolvedMirrorRefmaps {
				require.Contains(t, fetch, resolvedMirrorRefmap)
			}
		})
	}
}

func TestRemoveRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	require.NoError(t, Remove(ctx, testRepo, "origin"))

	repoPath, err := helper.GetRepoPath(testRepo)
	require.NoError(t, err)

	out := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	require.Len(t, out, 0)
}

func TestRemoveRemoteDontRemoveLocalBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath, err := helper.GetRepoPath(testRepo)
	require.NoError(t, err)

	//configure remote as fetch mirror
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "remote.origin.fetch", "+refs/*:refs/*")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "fetch")

	masterBeforeRemove := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "refs/heads/master")

	require.NoError(t, Remove(ctx, testRepo, "origin"))

	out := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "remote")
	require.Len(t, out, 0)

	out = testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "show-ref", "refs/heads/master")
	require.Equal(t, masterBeforeRemove, out)
}

func TestRemoteExists(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	found, err := Exists(ctx, testRepo, "origin")
	require.NoError(t, err)
	require.True(t, found)

	found, err = Exists(ctx, testRepo, "can-not-be-found")
	require.NoError(t, err)
	require.False(t, found)
}
