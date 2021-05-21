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
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func testSuccessfulAddRemote(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	_, repo, repoPath, client := setupRemoteServiceWithRuby(t, cfg, rubySrv)

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
				Repository:    repo,
				Name:          tc.remoteName,
				Url:           tc.url,
				MirrorRefmaps: tc.mirrorRefmaps,
			}

			_, err := client.AddRemote(ctx, request)
			require.NoError(t, err)

			remotes := gittest.Exec(t, cfg, "-C", repoPath, "remote", "-v")

			require.Contains(t, string(remotes), fmt.Sprintf("%s\t%s (fetch)", tc.remoteName, tc.url))
			require.Contains(t, string(remotes), fmt.Sprintf("%s\t%s (push)", tc.remoteName, tc.url))

			mirrorConfigRegexp := fmt.Sprintf("remote.%s", tc.remoteName)
			mirrorConfig := string(gittest.Exec(t, cfg, "-C", repoPath, "config", "--get-regexp", mirrorConfigRegexp))
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

func testAddRemoteTransactional(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.TxRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		var votes []voting.Vote
		txManager := transaction.MockManager{
			VoteFn: func(_ context.Context, _ txinfo.Transaction, _ txinfo.PraefectServer, vote voting.Vote) error {
				votes = append(votes, vote)
				return nil
			},
		}

		_, repo, repoPath, client := setupRemoteServiceWithRuby(t, cfg, rubySrv, testserver.WithTransactionManager(&txManager))

		ctx, err := (&txinfo.PraefectServer{SocketPath: "i-dont-care"}).Inject(ctx)
		require.NoError(t, err)
		ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
		require.NoError(t, err)
		ctx = helper.IncomingToOutgoing(ctx)

		preimageURL := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "remote", "get-url", "origin"))

		_, err = client.AddRemote(ctx, &gitalypb.AddRemoteRequest{
			Repository: repo,
			Name:       "origin",
			Url:        "foo/bar",
		})
		require.NoError(t, err)

		if featureflag.IsEnabled(ctx, featureflag.TxRemote) {
			preimageVote := fmt.Sprintf("remote.origin.url\t%s\n", preimageURL)
			require.Equal(t, []voting.Vote{
				voting.VoteFromData([]byte(preimageVote)),
				voting.VoteFromData([]byte("remote.origin.url\tfoo/bar\n")),
			}, votes)
		} else {
			require.Len(t, votes, 0)
		}
	})
}

func TestFailedAddRemoteDueToValidation(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

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
				Repository: repo,
				Name:       tc.remoteName,
				Url:        tc.url,
			}

			_, err := client.AddRemote(ctx, request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulRemoveRemote(t *testing.T) {
	cfg, repo, repoPath, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gittest.Exec(t, cfg, "-C", repoPath, "remote", "add", "my-remote", "http://my-repo.git")

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
				Repository: repo,
				Name:       tc.remoteName,
			}

			r, err := client.RemoveRemote(ctx, request)
			require.NoError(t, err)
			require.Equal(t, tc.result, r.GetResult())

			remotes := gittest.Exec(t, cfg, "-C", repoPath, "remote")

			require.NotContains(t, string(remotes), tc.remoteName)
		})
	}
}

func TestFailedRemoveRemoteDueToValidation(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.RemoveRemoteRequest{Repository: repo} // Remote name empty

	_, err := client.RemoveRemote(ctx, request)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestRemoveRemoteTransactional(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.TxRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		var votes []voting.Vote
		txManager := transaction.MockManager{
			VoteFn: func(_ context.Context, _ txinfo.Transaction, _ txinfo.PraefectServer, vote voting.Vote) error {
				votes = append(votes, vote)
				return nil
			},
		}

		cfg, repo, repoPath, client := setupRemoteService(t, testserver.WithTransactionManager(&txManager))

		ctx, err := (&txinfo.PraefectServer{SocketPath: "i-dont-care"}).Inject(ctx)
		require.NoError(t, err)
		ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
		require.NoError(t, err)
		ctx = helper.IncomingToOutgoing(ctx)

		preimageURL := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "remote", "get-url", "origin"))

		_, err = client.RemoveRemote(ctx, &gitalypb.RemoveRemoteRequest{
			Repository: repo,
			Name:       "origin",
		})
		require.NoError(t, err)

		if featureflag.IsEnabled(ctx, featureflag.TxRemote) {
			preimageVote := fmt.Sprintf("remote.origin.url\t%s\n", preimageURL)
			require.Equal(t, []voting.Vote{
				voting.VoteFromData([]byte(preimageVote)),
				voting.VoteFromData([]byte{}),
			}, votes)
		} else {
			require.Len(t, votes, 0)
		}
	})
}

func TestFindRemoteRepository(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoRefs := testhelper.MustReadFile(t, "testdata/lsremotedata.txt")
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		io.Copy(w, bytes.NewReader(infoRefs))
	}))
	defer ts.Close()

	resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: ts.URL, StorageName: repo.GetStorageName()})
	require.NoError(t, err)

	require.True(t, resp.Exists)
}

func TestFailedFindRemoteRepository(t *testing.T) {
	_, repo, _, client := setupRemoteService(t)

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
		resp, err := client.FindRemoteRepository(ctx, &gitalypb.FindRemoteRepositoryRequest{Remote: tc.remote, StorageName: repo.GetStorageName()})
		if tc.code == codes.OK {
			require.NoError(t, err)
		} else {
			testhelper.RequireGrpcError(t, err, tc.code)
			continue
		}

		require.Equal(t, tc.exists, resp.GetExists(), tc.description)
	}
}
