package repository

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestFetchRemoteSuccess(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, repoPath, client := setupRepositoryService(ctx, t)

	cloneRepo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	// Ensure there's a new tag to fetch
	gittest.WriteTag(t, cfg, repoPath, "testtag", "master")

	req := &gitalypb.FetchRemoteRequest{Repository: cloneRepo, RemoteParams: &gitalypb.Remote{
		Url: repoPath,
	}, Timeout: 120, CheckTagsChanged: true}
	resp, err := client.FetchRemote(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, resp.TagsChanged, true)

	// Ensure that it returns true if we're asked not to check
	req.CheckTagsChanged = false
	resp, err = client.FetchRemote(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, resp.TagsChanged, true)

	// Get the fetch status
	gittest.WriteTag(t, cfg, repoPath, "testtag2", "master")
	req.SendFetchStatus = true
	req.CheckTagsChanged = false
	resp, err = client.FetchRemote(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, resp.TagsChanged, true)

	status := resp.GetFetchStatus()
	require.Equal(t, 1, len(status))
	require.True(t, proto.Equal(&gitalypb.GitFetchStatus{
		Type:    gitalypb.GitFetchStatus_FETCHED,
		Summary: "[new tag]",
		From:    "testtag2",
		To:      "testtag2",
	}, status[0]))
	require.Empty(t, resp.FetchStatusParseError)

	resp, err = client.FetchRemote(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.GetFetchStatus())
	require.Equal(t, resp.TagsChanged, true)
}

func TestFetchRemote_sshCommand(t *testing.T) {
	testhelper.SkipWithPraefect(t, "It's not possible to create repositories through the API with the git command overwritten by the script.")

	t.Parallel()

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	outputPath := filepath.Join(testhelper.TempDir(t), "output")
	ctx := testhelper.Context(t)

	// We ain't got a nice way to intercept the SSH call, so we just write a custom git command
	// which simply prints the GIT_SSH_COMMAND environment variable.
	gitCmdFactory := gittest.NewInterceptingCommandFactory(ctx, t, cfg, func(git.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/bin/sh
			for arg in $GIT_SSH_COMMAND
			do
				case "$arg" in
				-oIdentityFile=*)
					path=$(echo "$arg" | cut -d= -f2)
					cat "$path";;
				*)
					echo "$arg";;
				esac
			done >'%s'
			exit 7
		`, outputPath)
	})

	client, _ := runRepositoryService(t, cfg, nil, testserver.WithGitCommandFactory(gitCmdFactory))

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.FetchRemoteRequest
		expectedOutput string
	}{
		{
			desc: "remote parameters without SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "https://example.com",
				},
			},
			expectedOutput: "ssh\n",
		},
		{
			desc: "remote parameters with SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "https://example.com",
				},
				SshKey: "mykey",
			},
			expectedOutput: "ssh\n-oIdentitiesOnly=yes\nmykey",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchRemote(ctx, tc.request)
			require.Error(t, err)
			require.Contains(t, err.Error(), "fetch remote: exit status 7")

			output := testhelper.MustReadFile(t, outputPath)
			require.Equal(t, tc.expectedOutput, string(output))

			require.NoError(t, os.Remove(outputPath))
		})
	}
}

func TestFetchRemote_withDefaultRefmaps(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, sourceRepoProto, sourceRepoPath, client := setupRepositoryService(ctx, t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	sourceRepo := localrepo.NewTestRepo(t, cfg, sourceRepoProto)

	targetRepoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})
	targetRepo := localrepo.NewTestRepo(t, cfg, targetRepoProto)

	port, stopGitServer := gittest.HTTPServer(ctx, t, gitCmdFactory, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/heads/foobar", "refs/heads/master", ""))

	// With no refmap given, FetchRemote should fall back to
	// "refs/heads/*:refs/heads/*" and thus mirror what's in the source
	// repository.
	resp, err := client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: targetRepoProto,
		RemoteParams: &gitalypb.Remote{
			Url: fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath)),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	sourceRefs, err := sourceRepo.GetReferences(ctx)
	require.NoError(t, err)
	targetRefs, err := targetRepo.GetReferences(ctx)
	require.NoError(t, err)
	require.Equal(t, sourceRefs, targetRefs)
}

func TestFetchRemote_transaction(t *testing.T) {
	t.Parallel()
	sourceCfg := testcfg.Build(t)

	txManager := transaction.NewTrackingManager()
	addr := testserver.RunGitalyServer(t, sourceCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
		))
	}, testserver.WithTransactionManager(txManager))
	sourceCfg.SocketPath = addr

	client := newRepositoryClient(t, sourceCfg, addr)

	ctx := testhelper.Context(t)
	_, sourceRepoPath := gittest.CreateRepository(ctx, t, sourceCfg, gittest.CreateRepositoryConfig{
		RelativePath: t.Name(),
		Seed:         gittest.SeedGitLabTest,
	})
	// Reset the manager as creating the repository casts some votes.
	txManager.Reset()

	targetCfg, targetRepoProto, targetRepoPath := testcfg.BuildWithRepo(t)
	targetGitCmdFactory := gittest.NewCommandFactory(t, targetCfg)

	port, stopGitServer := gittest.HTTPServer(ctx, t, targetGitCmdFactory, targetRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	require.Equal(t, 0, len(txManager.Votes()))

	_, err = client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: targetRepoProto,
		RemoteParams: &gitalypb.Remote{
			Url: fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath)),
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, len(txManager.Votes()))
}

func TestFetchRemote_prune(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, sourceRepoPath, client := setupRepositoryService(ctx, t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	port, stopGitServer := gittest.HTTPServer(ctx, t, gitCmdFactory, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	remoteURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath))

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.FetchRemoteRequest
		ref         git.ReferenceName
		shouldExist bool
	}{
		{
			desc: "NoPrune=true with explicit Remote should not delete reference",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
				},
				NoPrune: true,
			},
			ref:         "refs/heads/nonexistent",
			shouldExist: true,
		},
		{
			desc: "NoPrune=false with explicit Remote should delete reference",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
				},
				NoPrune: false,
			},
			ref:         "refs/heads/nonexistent",
			shouldExist: false,
		},
		{
			desc: "NoPrune=false with explicit Remote should not delete reference outside of refspec",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
					MirrorRefmaps: []string{
						"refs/heads/*:refs/remotes/my-remote/*",
					},
				},
				NoPrune: false,
			},
			ref:         "refs/heads/nonexistent",
			shouldExist: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			targetRepoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})
			targetRepo := localrepo.NewTestRepo(t, cfg, targetRepoProto)

			require.NoError(t, targetRepo.UpdateRef(ctx, tc.ref, "refs/heads/master", ""))

			tc.request.Repository = targetRepoProto
			resp, err := client.FetchRemote(ctx, tc.request)
			require.NoError(t, err)
			require.NotNil(t, resp)

			hasRevision, err := targetRepo.HasRevision(ctx, tc.ref.Revision())
			require.NoError(t, err)
			require.Equal(t, tc.shouldExist, hasRevision)
		})
	}
}

func TestFetchRemote_force(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, sourceRepoProto, sourceRepoPath, client := setupRepositoryService(ctx, t)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	sourceRepo := localrepo.NewTestRepo(t, cfg, sourceRepoProto)

	branchOID, err := sourceRepo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	tagOID, err := sourceRepo.ResolveRevision(ctx, "refs/tags/v1.0.0")
	require.NoError(t, err)

	divergingBranchOID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("b1"))
	divergingTagOID := gittest.WriteCommit(t, cfg, sourceRepoPath, gittest.WithBranch("b2"))

	port, stopGitServer := gittest.HTTPServer(ctx, t, gitCmdFactory, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	remoteURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath))

	for _, tc := range []struct {
		desc                string
		request             *gitalypb.FetchRemoteRequest
		expectedErr         error
		expectedRefs        map[git.ReferenceName]git.ObjectID
		expectedStatus      []gitalypb.GitFetchStatus
		expectedTagsChanged bool
	}{
		{
			desc: "remote params without force fails with diverging refs",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
				},
			},
			expectedErr: status.Error(codes.Unknown, "fetch remote: exit status 1"),
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": branchOID,
				"refs/tags/v1.0.0":  tagOID,
			},
		},
		{
			desc: "remote params with force updates diverging refs",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
				},
				Force: true,
			},
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": divergingBranchOID,
				"refs/tags/v1.0.0":  divergingTagOID,
			},
		},
		{
			desc: "remote params with force updates diverging refs with Git status",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
				},
				SendFetchStatus: true,
				Force:           true,
			},
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": divergingBranchOID,
				"refs/tags/v1.0.0":  divergingTagOID,
			},
			// * [new branch]      b1         -> b1
			// * [new branch]      b2         -> b2
			// + 1e292f8...cab056f master     -> master  (forced update)
			// t [tag update]      v1.0.0     -> v1.0.0
			expectedStatus: []gitalypb.GitFetchStatus{
				{Type: gitalypb.GitFetchStatus_FETCHED, Summary: "[new branch]", From: "b1", To: "b1"},
				{Type: gitalypb.GitFetchStatus_FETCHED, Summary: "[new branch]", From: "b2", To: "b2"},
				{Type: gitalypb.GitFetchStatus_FORCED_UPDATE, Reason: "(forced update)", Summary: "1e292f8...cab056f", From: "master", To: "master"},
				{Type: gitalypb.GitFetchStatus_TAG_UPDATE, Summary: "[tag update]", From: "v1.0.0", To: "v1.0.0"},
			},
			expectedTagsChanged: true,
		},
		{
			desc: "remote params with force-refmap fails with divergent tag",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
					MirrorRefmaps: []string{
						"+refs/heads/master:refs/heads/master",
					},
				},
			},
			// The master branch has been updated to the diverging branch, but the
			// command still fails because we do fetch tags by default, and the tag did
			// diverge.
			expectedErr: status.Error(codes.Unknown, "fetch remote: exit status 1"),
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": divergingBranchOID,
				"refs/tags/v1.0.0":  tagOID,
			},
		},
		{
			desc: "remote params with explicit refmap and force updates divergent tag",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
					MirrorRefmaps: []string{
						"refs/heads/master:refs/heads/master",
					},
				},
				Force: true,
			},
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": divergingBranchOID,
				"refs/tags/v1.0.0":  divergingTagOID,
			},
		},
		{
			desc: "remote params with force-refmap and no tags only updates refspec",
			request: &gitalypb.FetchRemoteRequest{
				RemoteParams: &gitalypb.Remote{
					Url: remoteURL,
					MirrorRefmaps: []string{
						"+refs/heads/master:refs/heads/master",
					},
				},
				NoTags: true,
			},
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": divergingBranchOID,
				"refs/tags/v1.0.0":  tagOID,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			targetRepoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			targetRepo := localrepo.NewTestRepo(t, cfg, targetRepoProto)

			// We're force-updating a branch and a tag in the source repository to point
			// to a diverging object ID in order to verify that the `force` parameter
			// takes effect.
			require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/heads/master", divergingBranchOID, branchOID))
			require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/tags/v1.0.0", divergingTagOID, tagOID))
			defer func() {
				// Restore references after the current testcase again. Moving
				// source repository setup into the testcases is not easily possible
				// because hosting the gitserver requires the repo path, and we need
				// the URL for our testcases.
				require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/heads/master", branchOID, divergingBranchOID))
				require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/tags/v1.0.0", tagOID, divergingTagOID))
			}()

			tc.request.Repository = targetRepoProto
			response, err := client.FetchRemote(ctx, tc.request)
			testhelper.RequireGrpcError(t, tc.expectedErr, err)

			updatedBranchOID, err := targetRepo.ResolveRevision(ctx, "refs/heads/master")
			require.NoError(t, err)
			updatedTagOID, err := targetRepo.ResolveRevision(ctx, "refs/tags/v1.0.0")
			require.NoError(t, err)

			require.Equal(t, map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": updatedBranchOID,
				"refs/tags/v1.0.0":  updatedTagOID,
			}, tc.expectedRefs)

			if tc.request.SendFetchStatus {
				require.Equal(t, tc.expectedTagsChanged, response.GetTagsChanged())
				require.Equal(t, len(tc.expectedStatus), len(response.GetFetchStatus()))
				for index, status := range response.GetFetchStatus() {
					require.True(t, proto.Equal(&tc.expectedStatus[index], status))
				}
			}
		})
	}
}

func TestFetchRemoteFailure(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	const remoteName = "test-repo"
	httpSrv, _ := remoteHTTPServer(t, remoteName, httpHost, httpToken)
	defer httpSrv.Close()

	tests := []struct {
		desc   string
		req    *gitalypb.FetchRemoteRequest
		code   codes.Code
		errMsg string
	}{
		{
			desc: "no repository",
			req: &gitalypb.FetchRemoteRequest{
				Repository: nil,
				RemoteParams: &gitalypb.Remote{
					Url: remoteName,
				},
				Timeout: 1000,
			},
			code:   codes.InvalidArgument,
			errMsg: "empty Repository",
		},
		{
			desc: "invalid storage",
			req: &gitalypb.FetchRemoteRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "invalid",
					RelativePath: "foobar.git",
				},
				RemoteParams: &gitalypb.Remote{
					Url: remoteName,
				},
				Timeout: 1000,
			},
			// the error text is shortened to only a single word as requests to gitaly done via praefect returns different error messages
			code:   codes.InvalidArgument,
			errMsg: "invalid",
		},
		{
			desc: "missing remote",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				Timeout:    1000,
			},
			code:   codes.InvalidArgument,
			errMsg: "missing remote params",
		},
		{
			desc: "invalid remote url",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "",
				},
				Timeout: 1000,
			},
			code:   codes.InvalidArgument,
			errMsg: `blank or empty remote URL`,
		},
		{
			desc: "not existing repo via http",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url:                     httpSrv.URL + "/invalid/repo/path.git",
					HttpAuthorizationHeader: httpToken,
					HttpHost:                httpHost,
					MirrorRefmaps:           []string{"all_refs"},
				},
				Timeout: 1000,
			},
			code:   codes.Unknown,
			errMsg: "invalid/repo/path.git/' not found",
		},
		{
			desc: "/dev/null",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url: "/dev/null",
				},
				Timeout: 1000,
			},
			code:   codes.Unknown,
			errMsg: "'/dev/null' does not appear to be a git repository",
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.FetchRemote(ctx, tc.req)
			require.Error(t, err)
			require.Nil(t, resp)

			require.Contains(t, err.Error(), tc.errMsg)
			testhelper.RequireGrpcCode(t, err, tc.code)
		})
	}
}

const (
	httpToken = "ABCefg0999182"
	httpHost  = "example.com"
)

func remoteHTTPServer(t *testing.T, repoName, httpHost, httpToken string) (*httptest.Server, string) {
	b := testhelper.MustReadFile(t, "testdata/advertise.txt")

	s := httptest.NewServer(
		// https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host != httpHost {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if r.URL.String() != fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			if httpToken != "" && r.Header.Get("Authorization") != httpToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			_, err := w.Write(b)
			assert.NoError(t, err)
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}

func getRefnames(t *testing.T, cfg config.Cfg, repoPath string) []string {
	result := gittest.Exec(t, cfg, "-C", repoPath, "for-each-ref", "--format", "%(refname:lstrip=2)")
	return strings.Split(text.ChompBytes(result), "\n")
}

func TestFetchRemoteOverHTTP(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, _, client := setupRepositoryService(ctx, t)

	testCases := []struct {
		description string
		httpToken   string
		remoteURL   string
	}{
		{
			description: "with http token",
			httpToken:   httpToken,
		},
		{
			description: "without http token",
			httpToken:   "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			forkedRepo, forkedRepoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
				Seed: gittest.SeedGitLabTest,
			})

			s, remoteURL := remoteHTTPServer(t, "my-repo", httpHost, tc.httpToken)
			defer s.Close()

			req := &gitalypb.FetchRemoteRequest{
				Repository: forkedRepo,
				RemoteParams: &gitalypb.Remote{
					Url:                     remoteURL,
					HttpAuthorizationHeader: tc.httpToken,
					HttpHost:                httpHost,
					MirrorRefmaps:           []string{"all_refs"},
				},
				Timeout: 1000,
			}
			if tc.remoteURL != "" {
				req.RemoteParams.Url = s.URL + tc.remoteURL
			}

			refs := getRefnames(t, cfg, forkedRepoPath)
			require.True(t, len(refs) > 1, "the advertisement.txt should have deleted all refs except for master")

			_, err := client.FetchRemote(ctx, req)
			require.NoError(t, err)

			refs = getRefnames(t, cfg, forkedRepoPath)

			require.Len(t, refs, 1)
			assert.Equal(t, "master", refs[0])
		})
	}
}

func TestFetchRemoteWithPath(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _, sourceRepoPath, client := setupRepositoryService(ctx, t)

	mirrorRepo, mirrorRepoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	_, err := client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: mirrorRepo,
		RemoteParams: &gitalypb.Remote{
			Url: sourceRepoPath,
		},
	})
	require.NoError(t, err)

	require.Equal(t, getRefnames(t, cfg, sourceRepoPath), getRefnames(t, cfg, mirrorRepoPath))
}

func TestFetchRemoteOverHTTPWithRedirect(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			http.Redirect(w, r, "/redirect_url", http.StatusSeeOther)
		}),
	)
	defer s.Close()

	req := &gitalypb.FetchRemoteRequest{
		Repository:   repo,
		RemoteParams: &gitalypb.Remote{Url: s.URL},
		Timeout:      1000,
	}

	_, err := client.FetchRemote(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "The requested URL returned error: 303")
}

func TestFetchRemoteOverHTTPWithTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(testhelper.Context(t))
	_, repo, _, client := setupRepositoryService(ctx, t)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			// Block the request forever.
			<-ctx.Done()
		}),
	)
	defer func() {
		// We need to explicitly cancel the context here, or otherwise we'll be stuck
		// closing the server due to the ongoing request.
		cancel()
		s.Close()
	}()

	req := &gitalypb.FetchRemoteRequest{
		Repository:   repo,
		RemoteParams: &gitalypb.Remote{Url: s.URL},
		Timeout:      1,
	}

	_, err := client.FetchRemote(ctx, req)
	require.Error(t, err)

	require.Contains(t, err.Error(), "fetch remote: signal: terminated")
}

func TestFetchStatusConversion(t *testing.T) {
	t.Parallel()

	expectedMappings := map[git.RefUpdateType]gitalypb.GitFetchStatus_Type{
		git.RefUpdateTypeFastForwardUpdate: gitalypb.GitFetchStatus_FAST_FORWARD_UPDATE,
		git.RefUpdateTypeForcedUpdate:      gitalypb.GitFetchStatus_FORCED_UPDATE,
		git.RefUpdateTypePruned:            gitalypb.GitFetchStatus_PRUNED,
		git.RefUpdateTypeTagUpdate:         gitalypb.GitFetchStatus_TAG_UPDATE,
		git.RefUpdateTypeFetched:           gitalypb.GitFetchStatus_FETCHED,
		git.RefUpdateTypeUpdateFailed:      gitalypb.GitFetchStatus_UPDATE_FAILED,
		git.RefUpdateTypeUnchanged:         gitalypb.GitFetchStatus_UNCHANGED,
	}

	for input, output := range expectedMappings {
		require.Equal(t, output, gitFetchStatusType(input))
	}
}
