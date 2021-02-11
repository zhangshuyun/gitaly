package conflicts_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

func TestSuccessfulResolveConflictsRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulResolveConflictsRequest(t, ctx)
	})
}

func testSuccessfulResolveConflictsRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := conflicts.NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()
	repo := localrepo.New(testRepo, config.Config)

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	mdGS := testhelper.GitalyServersMetadata(t, serverSocketPath)
	mdFF, _ := metadata.FromOutgoingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Join(mdGS, mdFF))

	filesJSON, err := json.Marshal(files)
	require.NoError(t, err)

	sourceBranch := "conflict-resolvable"
	headerRequest := &gitalypb.ResolveConflictsRequest{
		ResolveConflictsRequestPayload: &gitalypb.ResolveConflictsRequest_Header{
			Header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				TargetRepository: testRepo,
				CommitMessage:    []byte(conflictResolutionCommitMessage),
				OurCommitOid:     "1450cd639e0bc6721eb02800169e464f212cde06",
				TheirCommitOid:   "824be604a34828eb682305f0d963056cfac87b2d",
				SourceBranch:     []byte(sourceBranch),
				TargetBranch:     []byte("conflict-start"),
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

	headCommit, err := repo.ReadCommit(ctxOuter, git.Revision(sourceBranch))
	require.NoError(t, err)
	require.Contains(t, headCommit.ParentIds, "1450cd639e0bc6721eb02800169e464f212cde06")
	require.Contains(t, headCommit.ParentIds, "824be604a34828eb682305f0d963056cfac87b2d")
	require.Equal(t, string(headCommit.Author.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Committer.Email), "johndoe@gitlab.com")
	require.Equal(t, string(headCommit.Subject), conflictResolutionCommitMessage)
}

func TestResolveConflicts_stableID(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testResolveConflictsStableID(t, ctx)
	})
}

func testResolveConflictsStableID(t *testing.T, ctx context.Context) {
	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := conflicts.NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()
	repo := localrepo.New(repoProto, config.Config)

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
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

func TestFailedResolveConflictsRequestDueToResolutionError(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToResolutionError(t, ctx)
	})
}

func testFailedResolveConflictsRequestDueToResolutionError(t *testing.T, ctx context.Context) {
	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := conflicts.NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	mdGS := testhelper.GitalyServersMetadata(t, serverSocketPath)
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
				Repository:       testRepo,
				TargetRepository: testRepo,
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

func TestFailedResolveConflictsRequestDueToValidation(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoResolveConflicts,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedResolveConflictsRequestDueToValidation(t, ctx)
	})
}

func testFailedResolveConflictsRequestDueToValidation(t *testing.T, ctx context.Context) {
	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := conflicts.NewConflictsClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	mdGS := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ourCommitOid := "1450cd639e0bc6721eb02800169e464f212cde06"
	theirCommitOid := "824be604a34828eb682305f0d963056cfac87b2d"
	commitMsg := []byte(conflictResolutionCommitMessage)
	sourceBranch := []byte("conflict-resolvable")
	targetBranch := []byte("conflict-start")

	testCases := []struct {
		desc   string
		header *gitalypb.ResolveConflictsRequestHeader
		code   codes.Code
	}{
		{
			desc: "empty repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       nil,
				OurCommitOid:     ourCommitOid,
				TargetRepository: testRepo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty target repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: nil,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty OurCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     "",
				TargetRepository: testRepo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty TheirCommitId repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: testRepo,
				TheirCommitOid:   "",
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty CommitMessage repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: testRepo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    nil,
				SourceBranch:     sourceBranch,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty SourceBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: testRepo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     nil,
				TargetBranch:     targetBranch,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty TargetBranch repo",
			header: &gitalypb.ResolveConflictsRequestHeader{
				Repository:       testRepo,
				OurCommitOid:     ourCommitOid,
				TargetRepository: testRepo,
				TheirCommitOid:   theirCommitOid,
				CommitMessage:    commitMsg,
				SourceBranch:     sourceBranch,
				TargetBranch:     nil,
			},
			code: codes.InvalidArgument,
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
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func runFullServer(t *testing.T) (string, func()) {
	return testserver.RunGitalyServer(t, config.Config, conflicts.RubyServer)
}
