package conflicts_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	user = &gitalypb.User{
		Name:  []byte("John Doe"),
		Email: []byte("johndoe@gitlab.com"),
		GlId:  "user-1",
	}
	conflictResolutionCommitMessage = "Solve conflicts"

	files = []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"sections": map[string]string{
				"2f6fcd96b88b36ce98c38da085c795a27d92a3dd_14_14": "head",
			},
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9":   "head",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21": "origin",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_49_49": "origin",
			},
		},
	}
)

func TestWithRubyServer(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	t.Run("testSuccessfulResolveConflictsRequest", func(t *testing.T) { testSuccessfulResolveConflictsRequest(t, cfg, rubySrv) })
	t.Run("testResolveConflictsWithRemoteRepo", func(t *testing.T) { testResolveConflictsWithRemoteRepo(t, cfg, rubySrv) })
	t.Run("testResolveConflictsLineEndings", func(t *testing.T) { testResolveConflictsLineEndings(t, cfg, rubySrv) })
	t.Run("testResolveConflictsNonOIDRequests", func(t *testing.T) { testResolveConflictsNonOIDRequests(t, cfg, rubySrv) })
	t.Run("testResolveConflictsIdenticalContent", func(t *testing.T) { testResolveConflictsIdenticalContent(t, cfg, rubySrv) })
	t.Run("testResolveConflictsStableID", func(t *testing.T) { testResolveConflictsStableID(t, cfg, rubySrv) })
	t.Run("testFailedResolveConflictsRequestDueToResolutionError", func(t *testing.T) { testFailedResolveConflictsRequestDueToResolutionError(t, cfg, rubySrv) })
	t.Run("testFailedResolveConflictsRequestDueToValidation", func(t *testing.T) { testFailedResolveConflictsRequestDueToValidation(t, cfg, rubySrv) })
}

func testSuccessfulResolveConflictsRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulResolveConflictsRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulResolveConflictsRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, repoPath, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	mdGS := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	mdFF, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

	missingAncestorPath := "files/missing_ancestor.txt"
	files := []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"sections": map[string]string{
				"2f6fcd96b88b36ce98c38da085c795a27d92a3dd_14_14": "head",
			},
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9":   "head",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21": "origin",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_49_49": "origin",
			},
		},
		{
			"old_path": missingAncestorPath,
			"new_path": missingAncestorPath,
			"sections": map[string]string{
				"b760bfd3b1b1da380b4276eb30fb3b2b7e4f08e1_1_1": "origin",
			},
		},
	}

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	sourceBranch := "conflict-resolvable"
	targetBranch := "conflict-start"
	ourCommitOID := "1450cd639e0bc6721eb02800169e464f212cde06"   // part of branch conflict-resolvable
	theirCommitOID := "824be604a34828eb682305f0d963056cfac87b2d" // part of branch conflict-start
	ancestorCommitOID := "6907208d755b60ebeacb2e9dfea74c92c3449a1f"

	// introduce a conflict that exists on both branches, but not the
	// ancestor
	commitConflict := func(parentCommitID, branch, blob string) string {
		blobID, err := repo.WriteBlob(ctx, "", strings.NewReader(blob))
		require.NoError(t, err)
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "read-tree", branch)
		testhelper.MustRunCommand(t, nil,
			"git", "-C", repoPath,
			"update-index", "--add", "--cacheinfo", "100644", blobID.String(), missingAncestorPath,
		)
		treeID := bytes.TrimSpace(
			testhelper.MustRunCommand(t, nil,
				"git", "-C", repoPath, "write-tree",
			),
		)
		commitID := bytes.TrimSpace(
			testhelper.MustRunCommand(t, nil,
				"git", "-C", repoPath,
				"commit-tree", string(treeID), "-p", parentCommitID,
			),
		)
		testhelper.MustRunCommand(t, nil,
			"git", "-C", repoPath, "update-ref", "refs/heads/"+branch, string(commitID))
		return string(commitID)
	}

	// sanity check: make sure the conflict file does not exist on the
	// common ancestor
	cmd := exec.CommandContext(ctx, "git", "cat-file", "-e", ancestorCommitOID+":"+missingAncestorPath)
	require.Error(t, cmd.Run())

	ourCommitOID = commitConflict(ourCommitOID, sourceBranch, "content-1")
	theirCommitOID = commitConflict(theirCommitOID, targetBranch, "content-2")

	headerRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     ourCommitOID,
				TheirCommitOid:   theirCommitOID,
				SourceBranch:     []byte(sourceBranch),
				TargetBranch:     []byte(targetBranch),
				User:             user,
			},
		},
	}
	filesRequest1 := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON[:50],
		},
	}
	filesRequest2 := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON[50:],
		},
	}

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(filesRequest1))
	require.NoError(t, stream.Send(filesRequest2))

	r, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, r.GetResolutionError())

	headCommit, err := repo.ReadCommit(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)
	require.Contains(t, headCommit.ParentIds, ourCommitOID)
	require.Contains(t, headCommit.ParentIds, theirCommitOID)
	require.Equal(t, string(headCommit.Author.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Committer.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Subject), conflictResolutionCommitMessage)
}

func testResolveConflictsWithRemoteRepo(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsWithRemoteRepoFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsWithRemoteRepoFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, _, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	testhelper.ConfigureGitalySSHBin(t, cfg)
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	sourceRepo, sourceRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "source")
	t.Cleanup(cleanup)
	sourceBlobOID := gittest.WriteBlob(t, sourceRepoPath, []byte("contents-1\n"))
	sourceCommitOID := gittest.CommitBlobWithName(t, sourceRepoPath, sourceBlobOID.String(), "file.txt", "message")
	testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "update-ref", "refs/heads/source", sourceCommitOID)

	targetRepo, targetRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "target")
	t.Cleanup(cleanup)
	targetBlobOID := gittest.WriteBlob(t, targetRepoPath, []byte("contents-2\n"))
	targetCommitOID := gittest.CommitBlobWithName(t, targetRepoPath, targetBlobOID.String(), "file.txt", "message")
	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "update-ref", "refs/heads/target", targetCommitOID)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadata(t, cfg.SocketPath))

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)

	filesJSON, err := json.Marshal([]map[string]interface{}{
		{
			"old_path": "file.txt",
			"new_path": "file.txt",
			"sections": map[string]string{
				"5436437fa01a7d3e41d46741da54b451446774ca_1_1": "origin",
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       sourceRepo,
				TargetRepository: targetRepo,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     sourceCommitOID,
				TheirCommitOid:   targetCommitOID,
				SourceBranch:     []byte("source"),
				TargetBranch:     []byte("target"),
				User:             user,
			},
		},
	}))
	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, response.GetResolutionError())

	require.Equal(t, []byte("contents-2\n"), testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "cat-file", "-p", "refs/heads/source:file.txt"))
}

func testResolveConflictsLineEndings(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsLineEndingsFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsLineEndingsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, repoPath, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadata(t, cfg.SocketPath))

	for _, tc := range []struct {
		desc             string
		ourContent       string
		theirContent     string
		resolutions      []map[string]interface{}
		expectedContents string
	}{
		{
			desc:             "only newline",
			ourContent:       "\n",
			theirContent:     "\n",
			resolutions:      []map[string]interface{}{},
			expectedContents: "\n",
		},
		{
			desc:         "conflicting newline with embedded character",
			ourContent:   "\nA\n",
			theirContent: "\nB\n",
			resolutions: []map[string]interface{}{
				{
					"old_path": "file.txt",
					"new_path": "file.txt",
					"sections": map[string]string{
						"5436437fa01a7d3e41d46741da54b451446774ca_2_2": "head",
					},
				},
			},
			expectedContents: "\nA\n",
		},
		{
			desc:         "conflicting carriage-return newlines",
			ourContent:   "A\r\nB\r\nC\r\nD\r\nE\r\n",
			theirContent: "A\r\nB\r\nX\r\nD\r\nE\r\n",
			resolutions: []map[string]interface{}{
				{
					"old_path": "file.txt",
					"new_path": "file.txt",
					"sections": map[string]string{
						"5436437fa01a7d3e41d46741da54b451446774ca_3_3": "origin",
					},
				},
			},
			expectedContents: "A\r\nB\r\nX\r\nD\r\nE\r\n",
		},
		{
			desc:         "conflict with no trailing newline",
			ourContent:   "A\nB",
			theirContent: "X\nB",
			resolutions: []map[string]interface{}{
				{
					"old_path": "file.txt",
					"new_path": "file.txt",
					"sections": map[string]string{
						"5436437fa01a7d3e41d46741da54b451446774ca_1_1": "head",
					},
				},
			},
			expectedContents: "A\nB",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ourOID := gittest.WriteBlob(t, repoPath, []byte(tc.ourContent))
			ourCommit := gittest.CommitBlobWithName(t, repoPath, ourOID.String(), "file.txt", "message")
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/heads/ours", ourCommit)

			theirOID := gittest.WriteBlob(t, repoPath, []byte(tc.theirContent))
			theirCommit := gittest.CommitBlobWithName(t, repoPath, theirOID.String(), "file.txt", "message")
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/heads/theirs", theirCommit)

			stream, err := client.ResolveConflicts(ctx)
			require.NoError(t, err)

			filesJSON, err := json.Marshal(tc.resolutions)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
					Header: &gitalypb.ResolveConflictsRequestHeader{
						Repository:       repo,
						TargetRepository: repo,
						CommitMessage:    []byte(conflictResolutionCommitMessage),
						OurCommitOid:     ourCommit,
						TheirCommitOid:   theirCommit,
						SourceBranch:     []byte("ours"),
						TargetBranch:     []byte("theirs"),
						User:             user,
					},
				},
			}))
			require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
					FilesJson: filesJSON,
				},
			}))

			response, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Empty(t, response.GetResolutionError())

			require.Equal(t, []byte(tc.expectedContents), testhelper.MustRunCommand(t, nil,
				"git", "-C", repoPath, "cat-file", "-p", "refs/heads/ours:file.txt"))
		})
	}
}

func testResolveConflictsNonOIDRequests(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsNonOIDRequestsFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsNonOIDRequestsFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "conflict-resolvable",
				TheirCommitOid:   "conflict-start",
				SourceBranch:     []byte("conflict-resolvable"),
				TargetBranch:     []byte("conflict-start"),
				User:             user,
			},
		},
	}))

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}))

	_, err = stream.CloseAndRecv()
	require.Equal(t, status.Errorf(codes.Unknown, "Rugged::InvalidError: unable to parse OID - contains invalid characters"), err)
}

func testResolveConflictsIdenticalContent(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsIdenticalContentFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsIdenticalContentFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, repoPath, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	sourceBranch := "conflict-resolvable"
	sourceOID, err := repo.ResolveRevision(ctx, git.Revision(sourceBranch))
	require.NoError(t, err)

	targetBranch := "conflict-start"
	targetOID, err := repo.ResolveRevision(ctx, git.Revision(targetBranch))
	require.NoError(t, err)

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	var conflictingPaths []string
	for _, rev := range []string{
		sourceOID.String(),
		"6907208d755b60ebeacb2e9dfea74c92c3449a1f",
		targetOID.String(),
	} {
		contents := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "cat-file", "-p", rev+":files/ruby/popen.rb")
		path := filepath.Join(tempDir, rev)
		require.NoError(t, ioutil.WriteFile(path, contents, 0644))
		conflictingPaths = append(conflictingPaths, path)
	}

	var conflictContents bytes.Buffer
	err = repo.ExecAndWait(ctx, git.SubCmd{
		Name: "merge-file",
		Flags: []git.Option{
			git.Flag{"--quiet"},
			git.Flag{"--stdout"},
			// We pass `-L` three times for each of the conflicting files.
			git.ValueFlag{Name: "-L", Value: "files/ruby/popen.rb"},
			git.ValueFlag{Name: "-L", Value: "files/ruby/popen.rb"},
			git.ValueFlag{Name: "-L", Value: "files/ruby/popen.rb"},
		},
		Args: conflictingPaths,
	}, git.WithStdout(&conflictContents))

	// The merge will result in a merge conflict and thus cause the command to fail.
	require.Error(t, err)
	require.Contains(t, conflictContents.String(), "<<<<<<")

	filesJSON, err := json.Marshal([]map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"content":  conflictContents.String(),
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9":   "head",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21": "origin",
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_49_49": "origin",
			},
		},
	})
	require.NoError(t, err)

	ctx = testhelper.MergeOutgoingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))
	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     sourceOID.String(),
				TheirCommitOid:   targetOID.String(),
				SourceBranch:     []byte(sourceBranch),
				TargetBranch:     []byte(targetBranch),
				User:             user,
			},
		},
	}))
	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	testhelper.ProtoEqual(t, &gitalypb.ResolveConflictsResponse{
		ResolutionError: "Resolved content has no changes for file files/ruby/popen.rb",
	}, response)
}

func testResolveConflictsStableID(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsStableIDFeatured(t, ctx, cfg, rubySrv)
	})
}

func testResolveConflictsStableIDFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repoProto, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repoProto,
				TargetRepository: repoProto,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "1450cd639e0bc6721eb02800169e464f212cde06",
				TheirCommitOid:   "824be604a34828eb682305f0d963056cfac87b2d",
				SourceBranch:     []byte("conflict-resolvable"),
				TargetBranch:     []byte("conflict-start"),
				User:             user,
				Timestamp:        &timestamp.Timestamp{Seconds: 12345},
			},
		},
	}))

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}))

	response, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Empty(t, response.GetResolutionError())

	resolvedCommit, err := repo.ReadCommit(ctx, git.Revision("conflict-resolvable"))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:     "a5ad028fd739d7a054b07c293e77c5b7aecc2435",
		TreeId: "febd97e4a09e71355a513d7e0b0b3808e2dabd28",
		ParentIds: []string{
			"1450cd639e0bc6721eb02800169e464f212cde06",
			"824be604a34828eb682305f0d963056cfac87b2d",
		},
		Subject:  []byte(conflictResolutionCommitMessage),
		Body:     []byte(conflictResolutionCommitMessage),
		BodySize: 15,
		Author: &gitalypb.CommitAuthor{
			Name:     user.Name,
			Email:    user.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     user.Name,
			Email:    user.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, resolvedCommit)
}

func testFailedResolveConflictsRequestDueToResolutionError(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToResolutionErrorFeatured(t, ctx, cfg, rubySrv)
	})
}

func testFailedResolveConflictsRequestDueToResolutionErrorFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	mdGS := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	mdFF, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

	files := []map[string]interface{}{
		{
			"old_path": "files/ruby/popen.rb",
			"new_path": "files/ruby/popen.rb",
			"content":  "",
		},
		{
			"old_path": "files/ruby/regex.rb",
			"new_path": "files/ruby/regex.rb",
			"sections": map[string]string{
				"6eb14e00385d2fb284765eb1cd8d420d33d63fc9_9_9": "head",
			},
		},
	}
	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	headerRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       repo,
				TargetRepository: repo,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "1450cd639e0bc6721eb02800169e464f212cde06",
				TheirCommitOid:   "824be604a34828eb682305f0d963056cfac87b2d",
				SourceBranch:     []byte("conflict-resolvable"),
				TargetBranch:     []byte("conflict-start"),
				User:             user,
			},
		},
	}
	filesRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_FilesJson{
			FilesJson: filesJSON,
		},
	}

	stream, err := client.ResolveConflicts(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(filesRequest))

	r, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, r.GetResolutionError(), "Missing resolution for section ID: 6eb14e00385d2fb284765eb1cd8d420d33d63fc9_21_21")
}

func testFailedResolveConflictsRequestDueToValidation(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToValidationFeatured(t, ctx, cfg, rubySrv)
	})
}

func testFailedResolveConflictsRequestDueToValidationFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, _, client := conflicts.SetupConflictsServiceWithRuby(t, cfg, rubySrv, true)

	mdGS := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ourCommitOid := "1450cd639e0bc6721eb02800169e464f212cde06"
	theirCommitOid := "824be604a34828eb682305f0d963056cfac87b2d"
	commitMsg := []byte(conflictResolutionCommitMessage)
	sourceBranch := []byte("conflict-resolvable")
	targetBranch := []byte("conflict-start")

	testCases := []struct {
		desc         string
		header       *gitalypb.ResolveConflictsRequestHeader
		expectedCode codes.Code
		expectedErr  string
	}{
		{
			desc: "empty user",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             nil,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty User",
		},
		{
			desc: "empty repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       nil,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			// Praefect checks for an empty repository, too, but will raise a different
			// error message. Luckily, both Gitaly's and Praefect's error messages
			// contain "empty Repository".
			expectedErr: "empty Repository",
		},
		{
			desc: "empty target repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: nil,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TargetRepository",
		},
		{
			desc: "empty OurCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     "",
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty OurCommitOid",
		},
		{
			desc: "empty TheirCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   "",
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TheirCommitOid",
		},
		{
			desc: "empty CommitMessage repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    nil,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty CommitMessage",
		},
		{
			desc: "empty SourceBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     nil,
				TargetBranch:     targetBranch,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty SourceBranch",
		},
		{
			desc: "empty TargetBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				User:             user,
				Repository:       repo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: repo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     nil,
			},
			expectedCode: codes.InvalidArgument,
			expectedErr:  "ResolveConflicts: empty TargetBranch",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			mdFF, _ := metadata.FromOutgoingContext(ctx)
			ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

			stream, err := client.ResolveConflicts(ctx)
			require.NoError(t, err)

			headerRequest := &gitalypb.ResolveConflictsRequest{
				ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
					Header: testCase.header,
				},
			}
			require.NoError(t, stream.Send(headerRequest))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, testCase.expectedCode)
			require.Contains(t, err.Error(), testCase.expectedErr)
		})
	}
}
