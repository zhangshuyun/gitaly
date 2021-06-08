package remote

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testUpdateRemoteMirror(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUpdateRemoteMirror,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testUpdateRemoteMirrorFeatured(t, ctx, cfg, rubySrv)
	})
}

type commandFactoryWrapper struct {
	git.CommandFactory
	newFunc func(context.Context, repository.GitRepo, git.Cmd, ...git.CmdOpt) (*command.Command, error)
}

func (w commandFactoryWrapper) New(ctx context.Context, repo repository.GitRepo, sc git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return w.newFunc(ctx, repo, sc, opts...)
}

func testUpdateRemoteMirrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)

	type refs map[string][]string

	for _, tc := range []struct {
		desc                 string
		sourceRefs           refs
		sourceSymRefs        map[string]string
		mirrorRefs           refs
		mirrorSymRefs        map[string]string
		keepDivergentRefs    bool
		onlyBranchesMatching []string
		wrapCommandFactory   func(testing.TB, git.CommandFactory) git.CommandFactory
		requests             []*gitalypb.UpdateRemoteMirrorRequest
		errorContains        string
		response             *gitalypb.UpdateRemoteMirrorResponse
		expectedMirrorRefs   map[string]string
	}{
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3503
			desc: "empty mirror source fails",
			mirrorRefs: refs{
				"refs/heads/tags": {"commit 1"},
			},
			errorContains: "rpc error: code = Internal desc = close stream to gitaly-ruby: rpc error: code = Unknown desc = NoMethodError: undefined method `id' for nil:NilClass",
		},
		{
			desc:     "mirror is up to date",
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
				"refs/tags/tag":     "commit 1",
			},
		},
		{
			desc: "creates missing references",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
				"refs/tags/tag":     "commit 1",
			},
		},
		{
			desc: "updates outdated references",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1", "commit 2"},
				"refs/tags/tag":     {"commit 1", "commit 2"},
			},
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 2",
				"refs/tags/tag":     "commit 2",
			},
		},
		{
			desc: "deletes unneeded references",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/heads/branch": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
			},
		},
		{
			desc: "deletes unneeded references that match the branch selector",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/master":      {"commit 1"},
				"refs/heads/matched":     {"commit 1"},
				"refs/heads/not-matched": {"commit 1"},
				"refs/tags/tag":          {"commit 1"},
			},
			onlyBranchesMatching: []string{"matched"},
			response:             &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master":      "commit 1",
				"refs/heads/not-matched": "commit 1",
			},
		},
		{
			desc: "ignores diverged branches not matched by the branch selector",
			sourceRefs: refs{
				"refs/heads/matched":  {"commit 1"},
				"refs/heads/diverged": {"commit 1"},
			},
			onlyBranchesMatching: []string{"matched"},
			keepDivergentRefs:    true,
			mirrorRefs: refs{
				"refs/heads/matched":  {"commit 1"},
				"refs/heads/diverged": {"commit 2"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/matched":  "commit 1",
				"refs/heads/diverged": "commit 2",
			},
		},
		{
			desc: "does not delete refs with KeepDivergentRefs",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			keepDivergentRefs: true,
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/heads/branch": {"commit 1"},
				"refs/tags/tag":     {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
				"refs/heads/branch": "commit 1",
				"refs/tags/tag":     "commit 1",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3502
			desc: "updating branch called tag fails",
			sourceRefs: refs{
				"refs/heads/tag": {"commit 1", "commit 2"},
			},
			mirrorRefs: refs{
				"refs/heads/tag": {"commit 1"},
			},
			errorContains: "rpc error: code = Internal desc = close stream to gitaly-ruby: rpc error: code = Unknown desc = Gitlab::Git::CommandError: fatal: tag shorthand without <tag>",
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3504
			desc: "fails if tag and branch named the same",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
				"refs/tags/master":  {"commit 1"},
			},
			errorContains: "rpc error: code = Internal desc = close stream to gitaly-ruby: rpc error: code = Unknown desc = Gitlab::Git::CommandError: error: src refspec master matches more than one",
		},
		{
			desc: "only local branches are considered",
			sourceRefs: refs{
				"refs/heads/master":               {"commit 1"},
				"refs/remote/local-remote/branch": {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/remote/mirror-remote/branch": {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master":                "commit 1",
				"refs/remote/mirror-remote/branch": "commit 1",
			},
		},
		{
			desc: "creates branches matching selector",
			sourceRefs: refs{
				"refs/heads/matches":        {"commit 1"},
				"refs/heads/does-not-match": {"commit 2"},
				"refs/tags/tag":             {"commit 3"},
			},
			onlyBranchesMatching: []string{"matches"},
			response:             &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/matches": "commit 1",
				"refs/tags/tag":      "commit 3",
			},
		},
		{
			desc: "updates branches matching selector",
			sourceRefs: refs{
				"refs/heads/matches":        {"commit 1", "commit 2"},
				"refs/heads/does-not-match": {"commit 3", "commit 4"},
				"refs/tags/tag":             {"commit 6"},
			},
			mirrorRefs: refs{
				"refs/heads/matches":        {"commit 1"},
				"refs/heads/does-not-match": {"commit 3"},
				"refs/tags/tag":             {"commit 5"},
			},
			onlyBranchesMatching: []string{"matches"},
			response:             &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/matches":        "commit 2",
				"refs/heads/does-not-match": "commit 3",
				"refs/tags/tag":             "commit 6",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3509
			desc: "overwrites diverged references without KeepDivergentRefs",
			sourceRefs: refs{
				"refs/heads/non-diverged": {"commit 1", "commit 2"},
				"refs/heads/master":       {"commit 2"},
				"refs/tags/tag-1":         {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/non-diverged": {"commit 1"},
				"refs/heads/master":       {"commit 2", "ahead"},
				"refs/tags/tag-1":         {"commit 2"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/non-diverged": "commit 2",
				"refs/heads/master":       "commit 2",
				"refs/tags/tag-1":         "commit 1",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3509
			desc: "keeps diverged references with KeepDivergentRefs",
			sourceRefs: refs{
				"refs/heads/non-diverged": {"commit 1", "commit 2"},
				"refs/heads/master":       {"commit 2"},
				"refs/tags/tag-1":         {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/non-diverged": {"commit 1"},
				"refs/heads/master":       {"commit 2", "ahead"},
				"refs/tags/tag-1":         {"commit 2"},
			},
			keepDivergentRefs: true,
			response: &gitalypb.UpdateRemoteMirrorResponse{
				DivergentRefs: [][]byte{
					[]byte("refs/heads/master"),
					[]byte("refs/tags/tag-1"),
				},
			},
			expectedMirrorRefs: map[string]string{
				"refs/heads/non-diverged": "commit 2",
				"refs/heads/master":       "ahead",
				"refs/tags/tag-1":         "commit 2",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3508
			desc: "mirror is up to date with symbolic reference",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			sourceSymRefs: map[string]string{
				"refs/heads/symbolic-reference": "refs/heads/master",
			},
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3508
			desc: "updates branch pointed to by symbolic reference",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			sourceSymRefs: map[string]string{
				"refs/heads/symbolic-reference": "refs/heads/master",
			},
			onlyBranchesMatching: []string{"symbolic-reference"},
			response:             &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{
				"refs/heads/master": "commit 1",
			},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3508
			//
			// refs/heads/master gets removed but and a broken sym ref is left in
			// refs/heads/symbolic-reference
			desc: "removes symbolic ref target from mirror if not symbolic ref is not present locally",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			mirrorRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			mirrorSymRefs: map[string]string{
				"refs/heads/symbolic-reference": "refs/heads/master",
			},
			response:           &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: map[string]string{},
		},
		{
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3508
			desc: "fails with symbolic reference and target in the same push",
			sourceRefs: refs{
				"refs/heads/master": {"commit 1"},
			},
			sourceSymRefs: map[string]string{
				"refs/heads/symbolic-reference": "refs/heads/master",
			},
			errorContains: "remote: error: cannot lock ref 'refs/heads/master': reference already exists",
		},
		{
			desc: "push batching works",
			sourceRefs: func() refs {
				out := refs{}
				for i := 0; i < 2*pushBatchSize+1; i++ {
					out[fmt.Sprintf("refs/heads/branch-%d", i)] = []string{"commit 1"}
				}
				return out
			}(),
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: func() map[string]string {
				out := map[string]string{}
				for i := 0; i < 2*pushBatchSize+1; i++ {
					out[fmt.Sprintf("refs/heads/branch-%d", i)] = "commit 1"
				}
				return out
			}(),
		},
		{
			desc: "pushes default branch in the first batch",
			wrapCommandFactory: func(t testing.TB, original git.CommandFactory) git.CommandFactory {
				firstPush := true
				return commandFactoryWrapper{
					CommandFactory: original,
					newFunc: func(ctx context.Context, repo repository.GitRepo, sc git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
						if sc.Subcommand() == "push" && firstPush {
							firstPush = false
							args, err := sc.CommandArgs()
							assert.NoError(t, err)
							assert.Contains(t, args, "refs/heads/master", "first push should contain the default branch")
						}

						return original.New(ctx, repo, sc, opts...)
					},
				}
			},
			sourceRefs: func() refs {
				out := refs{"refs/heads/master": []string{"commit 1"}}
				for i := 0; i < 2*pushBatchSize; i++ {
					out[fmt.Sprintf("refs/heads/branch-%d", i)] = []string{"commit 1"}
				}
				return out
			}(),
			response: &gitalypb.UpdateRemoteMirrorResponse{},
			expectedMirrorRefs: func() map[string]string {
				out := map[string]string{"refs/heads/master": "commit 1"}
				for i := 0; i < 2*pushBatchSize; i++ {
					out[fmt.Sprintf("refs/heads/branch-%d", i)] = "commit 1"
				}
				return out
			}(),
		},
		{
			desc: "limits the number of divergent refs returned",
			sourceRefs: func() refs {
				out := refs{}
				for i := 0; i < maxDivergentRefs+1; i++ {
					out[fmt.Sprintf("refs/heads/branch-%03d", i)] = []string{"commit 1"}
				}
				return out
			}(),
			mirrorRefs: func() refs {
				out := refs{}
				for i := 0; i < maxDivergentRefs+1; i++ {
					out[fmt.Sprintf("refs/heads/branch-%03d", i)] = []string{"commit 2"}
				}
				return out
			}(),
			keepDivergentRefs: true,
			response: &gitalypb.UpdateRemoteMirrorResponse{
				DivergentRefs: func() [][]byte {
					out := make([][]byte, maxDivergentRefs)
					for i := range out {
						out[i] = []byte(fmt.Sprintf("refs/heads/branch-%03d", i))
					}
					return out
				}(),
			},
			expectedMirrorRefs: func() map[string]string {
				out := map[string]string{}
				for i := 0; i < maxDivergentRefs+1; i++ {
					out[fmt.Sprintf("refs/heads/branch-%03d", i)] = "commit 2"
				}
				return out
			}(),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, mirrorRepoPath, cleanMirrorRepo := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
			defer cleanMirrorRepo()

			sourceRepoPb, sourceRepoPath, cleanSourceRepo := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
			defer cleanSourceRepo()

			// configure the mirror repository as a remote in the source
			gittest.Exec(t, cfg, "-C", sourceRepoPath, "remote", "add", "mirror", mirrorRepoPath)

			// create identical commits in both repositories so we can use them for
			// the references
			commitSignature := git2go.NewSignature("Test Author", "author@example.com", time.Now())
			executor := git2go.New(filepath.Join(cfg.BinDir, "gitaly-git2go"), cfg.Git.BinPath)

			// construct the starting state of the repositories
			for repoPath, references := range map[string]refs{
				sourceRepoPath: tc.sourceRefs,
				mirrorRepoPath: tc.mirrorRefs,
			} {
				for reference, commits := range references {
					var commitOID git.ObjectID
					for _, commit := range commits {
						var err error
						commitOID, err = executor.Commit(ctx, git2go.CommitParams{
							Repository: repoPath,
							Author:     commitSignature,
							Committer:  commitSignature,
							Message:    commit,
							Parent:     commitOID.String(),
						})
						require.NoError(t, err)
					}

					gittest.Exec(t, cfg, "-C", repoPath, "update-ref", reference, commitOID.String())
				}
			}
			for repoPath, symRefs := range map[string]map[string]string{
				sourceRepoPath: tc.sourceSymRefs,
				mirrorRepoPath: tc.mirrorSymRefs,
			} {
				for symRef, targetRef := range symRefs {
					gittest.Exec(t, cfg, "-C", repoPath, "symbolic-ref", symRef, targetRef)
				}
			}

			addr := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
				cmdFactory := deps.GetGitCmdFactory()
				if tc.wrapCommandFactory != nil {
					cmdFactory = tc.wrapCommandFactory(t, deps.GetGitCmdFactory())
				}

				gitalypb.RegisterRemoteServiceServer(srv, NewServer(
					deps.GetCfg(),
					deps.GetRubyServer(),
					deps.GetLocator(),
					cmdFactory,
					deps.GetCatfileCache(),
					deps.GetTxManager(),
				))
			})

			client, conn := newRemoteClient(t, addr)
			defer conn.Close()

			stream, err := client.UpdateRemoteMirror(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.UpdateRemoteMirrorRequest{
				Repository:        sourceRepoPb,
				RefName:           "mirror",
				KeepDivergentRefs: tc.keepDivergentRefs,
			}))

			for _, pattern := range tc.onlyBranchesMatching {
				require.NoError(t, stream.Send(&gitalypb.UpdateRemoteMirrorRequest{
					OnlyBranchesMatching: [][]byte{[]byte(pattern)},
				}))
			}

			resp, err := stream.CloseAndRecv()
			if tc.errorContains != "" {
				testhelper.RequireGrpcError(t, err, codes.Internal)
				require.Contains(t, err.Error(), tc.errorContains)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.response, resp)

			// Check that the refs on the mirror now refer to the correct commits.
			// This is done by checking the commit messages as the commits are otherwise
			// the same.
			actualMirrorRefs := map[string]string{}

			refLines := strings.Split(text.ChompBytes(gittest.Exec(t, cfg, "-C", mirrorRepoPath, "for-each-ref", "--format=%(refname)%00%(contents:subject)")), "\n")
			for _, line := range refLines {
				if line == "" {
					continue
				}

				split := strings.Split(line, "\000")
				actualMirrorRefs[split[0]] = split[1]
			}

			require.Equal(t, tc.expectedMirrorRefs, actualMirrorRefs)
		})
	}
}

func testSuccessfulUpdateRemoteMirrorRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUpdateRemoteMirror,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulUpdateRemoteMirrorRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulUpdateRemoteMirrorRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
	})

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "mirror")
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_1"

	gittest.CreateTag(t, cfg, mirrorPath, "v0.0.1", "master", nil) // I needed another tag for the tests
	gittest.CreateTag(t, cfg, testRepoPath, "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98", nil)
	gittest.CreateTag(t, cfg, testRepoPath, "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9", &gittest.CreateTagOpts{
		Message: "Overriding tag", Force: true})

	// Create a commit that only exists in the mirror
	mirrorOnlyCommitOid := gittest.WriteCommit(t, cfg, mirrorPath, gittest.WithBranch("master"))
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
		gittest.Exec(t, cfg, gitArgs...)
	}

	newTagOid := string(gittest.Exec(t, cfg, "-C", testRepoPath, "rev-parse", "v1.0.0"))
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
	localRefs := string(gittest.Exec(t, cfg, "-C", testRepoPath, "for-each-ref"))
	require.NotContains(t, localRefs, mirrorOnlyCommitOid)

	mirrorRefs := string(gittest.Exec(t, cfg, "-C", mirrorPath, "for-each-ref"))

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

func testSuccessfulUpdateRemoteMirrorRequestWithWildcards(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUpdateRemoteMirror,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulUpdateRemoteMirrorRequestWithWildcardsFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulUpdateRemoteMirrorRequestWithWildcardsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
	})

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "mirror")
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

	gittest.CreateTag(t, cfg, testRepoPath, "new-tag", "60ecb67744cb56576c30214ff52294f8ce2def98", nil) // Add tag
	gittest.CreateTag(t, cfg, testRepoPath, "v1.0.0", "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		&gittest.CreateTagOpts{Message: "Overriding tag", Force: true}) // Update tag

	for _, args := range setupCommands {
		gitArgs := []string{"-C", testRepoPath}
		gitArgs = append(gitArgs, args...)
		gittest.Exec(t, cfg, gitArgs...)
	}

	// Workaround for https://gitlab.com/gitlab-org/gitaly/issues/1439
	// Create a tag on the remote to ensure it gets deleted later
	gittest.CreateTag(t, cfg, mirrorPath, "v1.2.0", "master", nil)

	newTagOid := string(gittest.Exec(t, cfg, "-C", testRepoPath, "rev-parse", "v1.0.0"))
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

	mirrorRefs := string(gittest.Exec(t, cfg, "-C", mirrorPath, "for-each-ref"))
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

func testUpdateRemoteMirrorInmemory(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
	})

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	localRepo, localPath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "local")
	defer cleanup()
	gittest.WriteCommit(t, cfg, localPath)

	_, remotePath, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "remote")
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("Ruby implementation fails gracefully", func(t *testing.T) {
		ctx := featureflag.OutgoingCtxWithFeatureFlagValue(ctx, featureflag.GoUpdateRemoteMirror, "false")

		stream, err := client.UpdateRemoteMirror(ctx)
		require.NoError(t, err)

		require.NoError(t, stream.Send(&gitalypb.UpdateRemoteMirrorRequest{
			Repository: localRepo,
			Remote: &gitalypb.UpdateRemoteMirrorRequest_Remote{
				Url: remotePath,
			},
		}))

		_, err = stream.CloseAndRecv()
		require.Equal(t, status.Error(codes.InvalidArgument, "in-memory remotes require `gitaly_go_update_remote_mirror` feature flag"), err)
	})

	t.Run("Go implementation succeeds", func(t *testing.T) {
		ctx := featureflag.OutgoingCtxWithFeatureFlags(ctx, featureflag.GoUpdateRemoteMirror)

		stream, err := client.UpdateRemoteMirror(ctx)
		require.NoError(t, err)

		require.NoError(t, stream.Send(&gitalypb.UpdateRemoteMirrorRequest{
			Repository: localRepo,
			Remote: &gitalypb.UpdateRemoteMirrorRequest_Remote{
				Url: remotePath,
			},
		}))

		response, err := stream.CloseAndRecv()
		require.NoError(t, err)
		testhelper.ProtoEqual(t, &gitalypb.UpdateRemoteMirrorResponse{}, response)

		localRefs := string(gittest.Exec(t, cfg, "-C", localPath, "for-each-ref"))
		remoteRefs := string(gittest.Exec(t, cfg, "-C", remotePath, "for-each-ref"))
		require.Equal(t, localRefs, remoteRefs)
	})
}

func testSuccessfulUpdateRemoteMirrorRequestWithKeepDivergentRefs(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUpdateRemoteMirror,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulUpdateRemoteMirrorRequestWithKeepDivergentRefsFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulUpdateRemoteMirrorRequestWithKeepDivergentRefsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
	})

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "source")
	defer cleanupFn()

	_, mirrorPath, mirrorCleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "mirror")
	defer mirrorCleanupFn()

	remoteName := "remote_mirror_1"

	gittest.CreateTag(t, cfg, mirrorPath, "v2.0.0", "master", nil)

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
		gittest.Exec(t, cfg, gitArgs...)
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

	mirrorRefs := string(gittest.Exec(t, cfg, "-C", mirrorPath, "for-each-ref"))

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

	mirrorRefs = string(gittest.Exec(t, cfg, "-C", mirrorPath, "for-each-ref"))

	// Verify `master` gets overwritten with the value from the source
	require.Contains(t, mirrorRefs, "ba3faa7dbecdb555c748b36e8bc0f427e69de5e7 commit\trefs/heads/master")

	// Verify a branch only on the mirror is now deleted
	require.NotContains(t, mirrorRefs, "refs/heads/not-merged-branch")
}

func testFailedUpdateRemoteMirrorRequestDueToValidation(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUpdateRemoteMirror,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedUpdateRemoteMirrorRequestDueToValidationFeatured(t, ctx, cfg, rubySrv)
	})
}

func testFailedUpdateRemoteMirrorRequestDueToValidationFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	serverSocketPath := testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRemoteServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetTxManager(),
		))
	})

	client, conn := newRemoteClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
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
		{
			desc: "remote is missing URL",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: testRepo,
				Remote: &gitalypb.UpdateRemoteMirrorRequest_Remote{
					Url: "",
				},
			},
		},
		{
			desc: "both remote name and remote parameters set",
			request: &gitalypb.UpdateRemoteMirrorRequest{
				Repository: testRepo,
				RefName:    "foobar",
				Remote:     &gitalypb.UpdateRemoteMirrorRequest_Remote{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UpdateRemoteMirror(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(tc.request))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}
