package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func copyRepoWithNewRemote(t *testing.T, repo *gitalypb.Repository, repoPath string, remote string) (*gitalypb.Repository, string) {
	cloneRepo := &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "fetch-remote-clone.git"}

	clonePath := filepath.Join(filepath.Dir(repoPath), "fetch-remote-clone.git")
	require.NoError(t, os.RemoveAll(clonePath))

	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", repoPath, clonePath)

	testhelper.MustRunCommand(t, nil, "git", "-C", clonePath, "remote", "add", remote, repoPath)

	return cloneRepo, clonePath
}

func TestFetchRemoteSuccess(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	cloneRepo, cloneRepoPath := copyRepoWithNewRemote(t, repo, repoPath, "my-remote")
	defer func() {
		require.NoError(t, os.RemoveAll(cloneRepoPath))
	}()

	// Ensure there's a new tag to fetch
	testhelper.CreateTag(t, repoPath, "testtag", "master", nil)

	req := &gitalypb.FetchRemoteRequest{Repository: cloneRepo, Remote: "my-remote", Timeout: 120, CheckTagsChanged: true}
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
}

func TestFetchRemote_sshCommand(t *testing.T) {
	tempDir := testhelper.TempDir(t)

	// We ain't got a nice way to intercept the SSH call, so we just write a custom git command
	// which simply prints the GIT_SSH_COMMAND environment variable.
	gitPath := filepath.Join(tempDir, "git")
	outputPath := filepath.Join(tempDir, "output")
	script := fmt.Sprintf(`#!/bin/sh
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
	exit 7`, outputPath)
	testhelper.WriteExecutable(t, gitPath, []byte(script))

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	// We re-define path to the git executable to catch parameters used to call it.
	// This replacement only needs to be done for the configuration used to invoke git commands.
	// Other operations should use actual path to the git binary to work properly.
	spyGitCfg := cfg
	spyGitCfg.Git.BinPath = gitPath

	client, _ := runRepositoryService(t, spyGitCfg, nil)

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc           string
		request        *gitalypb.FetchRemoteRequest
		expectedOutput string
	}{
		{
			desc: "remote name without SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				Remote:     "my-remote",
			},
			expectedOutput: "ssh\n",
		},
		{
			desc: "remote name with SSH key",
			request: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				Remote:     "my-remote",
				SshKey:     "mykey",
			},
			expectedOutput: "ssh\n-oIdentitiesOnly=yes\nmykey",
		},
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
	cfg, sourceRepoProto, sourceRepoPath, client := setupRepositoryService(t)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	sourceRepo := localrepo.New(gitCmdFactory, sourceRepoProto, cfg)

	targetRepoProto, targetRepoPath := copyRepoWithNewRemote(t, sourceRepoProto, sourceRepoPath, "my-remote")
	defer func() {
		require.NoError(t, os.RemoveAll(targetRepoPath))
	}()
	targetRepo := localrepo.New(gitCmdFactory, targetRepoProto, cfg)

	port, stopGitServer := gittest.GitServer(t, cfg, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	ctx, cancel := testhelper.Context()
	defer cancel()

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

type mockTxManager struct {
	transaction.Manager
	votes int
}

func (m *mockTxManager) Vote(context.Context, metadata.Transaction, metadata.PraefectServer, transaction.Vote) error {
	m.votes++
	return nil
}

func TestFetchRemote_transaction(t *testing.T) {
	sourceCfg, _, sourceRepoPath := testcfg.BuildWithRepo(t)

	txManager := &mockTxManager{}
	addr := testserver.RunGitalyServer(t, sourceCfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(deps.GetCfg(), deps.GetRubyServer(), deps.GetLocator(), deps.GetTxManager(), deps.GetGitCmdFactory()))
	}, testserver.WithTransactionManager(txManager))

	client := newRepositoryClient(t, sourceCfg, addr)

	targetCfg, targetRepoProto, targetRepoPath := testcfg.BuildWithRepo(t)
	port, stopGitServer := gittest.GitServer(t, targetCfg, targetRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx, err := metadata.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx, err = (&metadata.PraefectServer{SocketPath: "i-dont-care"}).Inject(ctx)
	require.NoError(t, err)
	ctx = helper.IncomingToOutgoing(ctx)

	require.Equal(t, 0, txManager.votes)

	_, err = client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
		Repository: targetRepoProto,
		RemoteParams: &gitalypb.Remote{
			Url: fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath)),
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, txManager.votes)
}

func TestFetchRemote_prune(t *testing.T) {
	cfg, sourceRepo, sourceRepoPath, client := setupRepositoryService(t)

	port, stopGitServer := gittest.GitServer(t, cfg, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	remoteURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath))

	for _, tc := range []struct {
		desc        string
		request     *gitalypb.FetchRemoteRequest
		ref         git.ReferenceName
		shouldExist bool
	}{
		{
			desc: "NoPrune=true should not delete reference matching remote's refspec",
			request: &gitalypb.FetchRemoteRequest{
				Remote:  "my-remote",
				NoPrune: true,
			},
			ref:         "refs/remotes/my-remote/nonexistent",
			shouldExist: true,
		},
		{
			desc: "NoPrune=false should delete reference matching remote's refspec",
			request: &gitalypb.FetchRemoteRequest{
				Remote:  "my-remote",
				NoPrune: false,
			},
			ref:         "refs/remotes/my-remote/nonexistent",
			shouldExist: false,
		},
		{
			desc: "NoPrune=false should not delete ref outside of remote's refspec",
			request: &gitalypb.FetchRemoteRequest{
				Remote:  "my-remote",
				NoPrune: false,
			},
			ref:         "refs/heads/nonexistent",
			shouldExist: true,
		},
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
			targetRepoProto, targetRepoPath := copyRepoWithNewRemote(t, sourceRepo, sourceRepoPath, "my-remote")
			defer func() {
				require.NoError(t, os.RemoveAll(targetRepoPath))
			}()
			targetRepo := localrepo.New(git.NewExecCommandFactory(cfg), targetRepoProto, cfg)

			ctx, cancel := testhelper.Context()
			defer cancel()

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
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, sourceRepoProto, sourceRepoPath, client := setupRepositoryService(t)
	gitCommandFactory := git.NewExecCommandFactory(cfg)

	sourceRepo := localrepo.New(gitCommandFactory, sourceRepoProto, cfg)

	branchOID, err := sourceRepo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	tagOID, err := sourceRepo.ResolveRevision(ctx, "refs/tags/v1.0.0")
	require.NoError(t, err)

	divergingBranchOID, _ := gittest.CreateCommitOnNewBranch(t, cfg, sourceRepoPath)
	divergingTagOID, _ := gittest.CreateCommitOnNewBranch(t, cfg, sourceRepoPath)

	port, stopGitServer := gittest.GitServer(t, cfg, sourceRepoPath, nil)
	defer func() { require.NoError(t, stopGitServer()) }()

	remoteURL := fmt.Sprintf("http://127.0.0.1:%d/%s", port, filepath.Base(sourceRepoPath))

	for _, tc := range []struct {
		desc         string
		request      *gitalypb.FetchRemoteRequest
		expectedErr  error
		expectedRefs map[git.ReferenceName]git.ObjectID
	}{
		{
			desc: "remote without force fails with diverging refs",
			request: &gitalypb.FetchRemoteRequest{
				Remote: "my-remote",
			},
			expectedErr: status.Error(codes.Unknown, "fetch remote: exit status 1"),
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": branchOID,
				"refs/tags/v1.0.0":  tagOID,
			},
		},
		{
			desc: "remote with force updates diverging refs",
			request: &gitalypb.FetchRemoteRequest{
				Remote: "my-remote",
				Force:  true,
			},
			// We're fetching from `my-remote` here, which is set up to have a default
			// refspec of "+refs/heads/*:refs/remotes/foobar/*". As such, no normal
			// branches would get updated.
			expectedRefs: map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": branchOID,
				"refs/tags/v1.0.0":  git.ObjectID(divergingTagOID),
			},
		},
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
				"refs/heads/master": git.ObjectID(divergingBranchOID),
				"refs/tags/v1.0.0":  git.ObjectID(divergingTagOID),
			},
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
				"refs/heads/master": git.ObjectID(divergingBranchOID),
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
				"refs/heads/master": git.ObjectID(divergingBranchOID),
				"refs/tags/v1.0.0":  git.ObjectID(divergingTagOID),
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
				"refs/heads/master": git.ObjectID(divergingBranchOID),
				"refs/tags/v1.0.0":  tagOID,
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			targetRepoProto, targetRepoPath := copyRepoWithNewRemote(t, sourceRepoProto, sourceRepoPath, "my-remote")
			defer func() {
				require.NoError(t, os.RemoveAll(targetRepoPath))
			}()

			targetRepo := localrepo.New(gitCommandFactory, targetRepoProto, cfg)

			// We're force-updating a branch and a tag in the source repository to point
			// to a diverging object ID in order to verify that the `force` parameter
			// takes effect.
			require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/heads/master", git.ObjectID(divergingBranchOID), branchOID))
			require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/tags/v1.0.0", git.ObjectID(divergingTagOID), tagOID))
			defer func() {
				// Restore references after the current testcase again. Moving
				// source repository setup into the testcases is not easily possible
				// because hosting the gitserver requires the repo path, and we need
				// the URL for our testcases.
				require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/heads/master", branchOID, git.ObjectID(divergingBranchOID)))
				require.NoError(t, sourceRepo.UpdateRef(ctx, "refs/tags/v1.0.0", tagOID, git.ObjectID(divergingTagOID)))
			}()

			tc.request.Repository = targetRepoProto
			_, err := client.FetchRemote(ctx, tc.request)
			require.Equal(t, tc.expectedErr, err)

			updatedBranchOID, err := targetRepo.ResolveRevision(ctx, "refs/heads/master")
			require.NoError(t, err)
			updatedTagOID, err := targetRepo.ResolveRevision(ctx, "refs/tags/v1.0.0")
			require.NoError(t, err)

			require.Equal(t, map[git.ReferenceName]git.ObjectID{
				"refs/heads/master": updatedBranchOID,
				"refs/tags/v1.0.0":  updatedTagOID,
			}, tc.expectedRefs)
		})
	}
}

func testFetchRemoteFailure(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	_, repo, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	const remoteName = "test-repo"
	httpSrv, _ := remoteHTTPServer(t, remoteName, httpToken)
	defer httpSrv.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

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
				Remote:     remoteName,
				Timeout:    1000,
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
				Remote:  remoteName,
				Timeout: 1000,
			},
			// the error text is shortened to only a single word as requests to gitaly done via praefect returns different error messages
			code:   codes.InvalidArgument,
			errMsg: "invalid",
		},
		{
			desc: "invalid remote",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				Remote:     "",
				Timeout:    1000,
			},
			code:   codes.InvalidArgument,
			errMsg: `blank or empty "remote"`,
		},
		{
			desc: "invalid remote url: bad format",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url:                     "not a url",
					HttpAuthorizationHeader: httpToken,
				},
				Timeout: 1000,
			},
			code:   codes.InvalidArgument,
			errMsg: `invalid "remote_params.url"`,
		},
		{
			desc: "invalid remote url: no host",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url:                     "/not/a/url",
					HttpAuthorizationHeader: httpToken,
				},
				Timeout: 1000,
			},
			code:   codes.InvalidArgument,
			errMsg: `invalid "remote_params.url"`,
		},
		{
			desc: "not existing repo via http",
			req: &gitalypb.FetchRemoteRequest{
				Repository: repo,
				RemoteParams: &gitalypb.Remote{
					Url:                     httpSrv.URL + "/invalid/repo/path.git",
					HttpAuthorizationHeader: httpToken,
					MirrorRefmaps:           []string{"all_refs"},
				},
				Timeout: 1000,
			},
			code:   codes.Unknown,
			errMsg: "invalid/repo/path.git/' not found",
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.FetchRemote(ctx, tc.req)
			require.Error(t, err)
			require.Nil(t, resp)

			require.Contains(t, err.Error(), tc.errMsg)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

const (
	httpToken = "ABCefg0999182"
)

func remoteHTTPServer(t *testing.T, repoName, httpToken string) (*httptest.Server, string) {
	b, err := ioutil.ReadFile("testdata/advertise.txt")
	require.NoError(t, err)

	s := httptest.NewServer(
		// https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.String() != fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			if httpToken != "" && r.Header.Get("Authorization") != httpToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			_, err = w.Write(b)
			assert.NoError(t, err)
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}

func getRefnames(t *testing.T, repoPath string) []string {
	result := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref", "--format", "%(refname:lstrip=2)")
	return strings.Split(text.ChompBytes(result), "\n")
}

func testFetchRemoteOverHTTP(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, _, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	ctx, cancel := testhelper.Context()
	defer cancel()

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
			forkedRepo, forkedRepoPath, forkedRepoCleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
			defer forkedRepoCleanup()

			s, remoteURL := remoteHTTPServer(t, "my-repo", tc.httpToken)
			defer s.Close()

			req := &gitalypb.FetchRemoteRequest{
				Repository: forkedRepo,
				RemoteParams: &gitalypb.Remote{
					Url:                     remoteURL,
					HttpAuthorizationHeader: tc.httpToken,
					MirrorRefmaps:           []string{"all_refs"},
				},
				Timeout: 1000,
			}
			if tc.remoteURL != "" {
				req.RemoteParams.Url = s.URL + tc.remoteURL
			}

			refs := getRefnames(t, forkedRepoPath)
			require.True(t, len(refs) > 1, "the advertisement.txt should have deleted all refs except for master")

			_, err := client.FetchRemote(ctx, req)
			require.NoError(t, err)

			refs = getRefnames(t, forkedRepoPath)

			require.Len(t, refs, 1)
			assert.Equal(t, "master", refs[0])
		})
	}
}

func TestFetchRemoteOverHTTPWithRedirect(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			http.Redirect(w, r, "/redirect_url", http.StatusSeeOther)
		}),
	)
	defer s.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

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
	_, repo, _, client := setupRepositoryService(t)

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			time.Sleep(2 * time.Second)
			http.Error(w, "", http.StatusNotFound)
		}),
	)
	defer s.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.FetchRemoteRequest{
		Repository:   repo,
		RemoteParams: &gitalypb.Remote{Url: s.URL},
		Timeout:      1,
	}

	_, err := client.FetchRemote(ctx, req)
	require.Error(t, err)

	require.Contains(t, err.Error(), "fetch remote: signal: terminated")
}
