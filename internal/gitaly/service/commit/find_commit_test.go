package commit

import (
	"bufio"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSuccessfulFindCommitRequest(t *testing.T) {
	windows1251Message := testhelper.MustReadFile(t, "testdata/commit-c809470461118b7bcab850f6e9a7ca97ac42f8ea-message.txt")

	cfg, repoProto, repoPath, client := setupCommitServiceWithRepo(t, true)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	bigMessage := "An empty commit with REALLY BIG message\n\n" + strings.Repeat("MOAR!\n", 20*1024)
	bigCommitID := gittest.WriteCommit(t, cfg, repoPath,
		gittest.WithBranch("local-big-commits"), gittest.WithMessage(bigMessage),
		gittest.WithParents("60ecb67744cb56576c30214ff52294f8ce2def98"),
	)
	bigCommit, err := repo.ReadCommit(ctx, git.Revision(bigCommitID))
	require.NoError(t, err)

	testCases := []struct {
		description string
		revision    string
		trailers    bool
		commit      *gitalypb.GitCommit
	}{
		{
			description: "With a branch name",
			revision:    "branch-merged",
			commit:      testhelper.GitLabTestCommit("498214de67004b1da3d820901307bed2a68a8ef6"),
		},
		{
			description: "With a tag name no trailers",
			revision:    "v1.0.0",
			trailers:    false,
			commit: &gitalypb.GitCommit{
				Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				Subject: []byte("More submodules"),
				Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				ParentIds:     []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
				BodySize:      84,
				SignatureType: gitalypb.SignatureType_PGP,
				TreeId:        "70d69cce111b0e1f54f7e5438bbbba9511a8e23c",
			},
		},
		{
			description: "With a tag name and trailers",
			revision:    "v1.0.0",
			trailers:    true,
			commit: &gitalypb.GitCommit{
				Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				Subject: []byte("More submodules"),
				Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				ParentIds:     []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
				BodySize:      84,
				SignatureType: gitalypb.SignatureType_PGP,
				TreeId:        "70d69cce111b0e1f54f7e5438bbbba9511a8e23c",
				Trailers: []*gitalypb.CommitTrailer{
					&gitalypb.CommitTrailer{
						Key:   []byte("Signed-off-by"),
						Value: []byte("Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>"),
					},
				},
			},
		},
		{
			description: "With a hash",
			revision:    "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			commit:      testhelper.GitLabTestCommit("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
		},
		{
			description: "With an initial commit",
			revision:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			commit:      testhelper.GitLabTestCommit("1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"),
		},
		{
			description: "More submodules",
			revision:    "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
			trailers:    true,
			commit: &gitalypb.GitCommit{
				Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				Subject: []byte("More submodules"),
				Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Dmitriy Zaporozhets"),
					Email:    []byte("dmitriy.zaporozhets@gmail.com"),
					Date:     &timestamp.Timestamp{Seconds: 1393491261},
					Timezone: []byte("+0200"),
				},
				ParentIds:     []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
				BodySize:      84,
				SignatureType: gitalypb.SignatureType_PGP,
				TreeId:        "70d69cce111b0e1f54f7e5438bbbba9511a8e23c",
				Trailers: []*gitalypb.CommitTrailer{
					&gitalypb.CommitTrailer{
						Key:   []byte("Signed-off-by"),
						Value: []byte("Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>"),
					},
				},
			},
		},
		{
			description: "with non-utf8 message encoding, recognized by Git",
			revision:    "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
			commit: &gitalypb.GitCommit{
				Id:      "c809470461118b7bcab850f6e9a7ca97ac42f8ea",
				Subject: windows1251Message[:len(windows1251Message)-1],
				Body:    windows1251Message,
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Jacob Vosmaer"),
					Email:    []byte("jacob@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1512132977},
					Timezone: []byte("+0100"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Jacob Vosmaer"),
					Email:    []byte("jacob@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1512132977},
					Timezone: []byte("+0100"),
				},
				ParentIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
				BodySize:  49,
				TreeId:    "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
			},
		},
		{
			description: "with non-utf8 garbage message encoding, not recognized by Git",
			revision:    "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
			commit: &gitalypb.GitCommit{
				Id:      "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
				Subject: []byte("Hello\xf0world"),
				Body:    []byte("Hello\xf0world\n"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Jacob Vosmaer"),
					Email:    []byte("jacob@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1517328273},
					Timezone: []byte("+0100"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Jacob Vosmaer"),
					Email:    []byte("jacob@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1517328273},
					Timezone: []byte("+0100"),
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				BodySize:  12,
				TreeId:    "7e2f26d033ee47cd0745649d1a28277c56197921",
			},
		},
		{
			description: "with a very large message",
			revision:    bigCommitID.String(),
			commit: &gitalypb.GitCommit{
				Id:      bigCommitID.String(),
				Subject: []byte("An empty commit with REALLY BIG message"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Scrooge McDuck"),
					Email:    []byte("scrooge@mcduck.com"),
					Date:     &timestamp.Timestamp{Seconds: bigCommit.Author.Date.Seconds},
					Timezone: []byte("+0100"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Scrooge McDuck"),
					Email:    []byte("scrooge@mcduck.com"),
					Date:     &timestamp.Timestamp{Seconds: bigCommit.Committer.Date.Seconds},
					Timezone: []byte("+0100"),
				},
				ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
				Body:      []byte(bigMessage[:helper.MaxCommitOrTagMessageSize]),
				BodySize:  int64(len(bigMessage)),
				TreeId:    "7e2f26d033ee47cd0745649d1a28277c56197921",
			},
		},
		{
			description: "with different author and committer",
			revision:    "77e835ef0856f33c4f0982f84d10bdb0567fe440",
			commit:      testhelper.GitLabTestCommit("77e835ef0856f33c4f0982f84d10bdb0567fe440"),
		},
		{
			description: "With a non-existing ref name",
			revision:    "this-doesnt-exists",
			commit:      nil,
		},
		{
			description: "With a non-existing hash",
			revision:    "f48214de67004b1da3d820901307bed2a68a8ef6",
			commit:      nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.FindCommitRequest{
				Repository: repoProto,
				Revision:   []byte(testCase.revision),
				Trailers:   testCase.trailers,
			}

			response, err := client.FindCommit(ctx, request)
			require.NoError(t, err)

			testhelper.ProtoEqual(t, testCase.commit, response.Commit)
		})
	}
}

func TestFailedFindCommitRequest(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description string
		revision    []byte
		repo        *gitalypb.Repository
	}{
		{repo: invalidRepo, revision: []byte("master"), description: "Invalid repo"},
		{repo: repo, revision: []byte(""), description: "Empty revision"},
		{repo: repo, revision: []byte("-master"), description: "Invalid revision"},
		{repo: repo, revision: []byte("mas:ter"), description: "Invalid revision"},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &gitalypb.FindCommitRequest{
				Repository: testCase.repo,
				Revision:   testCase.revision,
			}

			_, err := client.FindCommit(ctx, request)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "default lookup should fail")
		})
	}
}

func BenchmarkFindCommitNoCache(b *testing.B) {
	benchmarkFindCommit(false, b)
}

func BenchmarkFindCommitWithCache(b *testing.B) {
	benchmarkFindCommit(true, b)
}

func benchmarkFindCommit(withCache bool, b *testing.B) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, _, client := setupCommitServiceWithRepo(b, false)

	// get a list of revisions
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	logCmd, err := gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "log", Flags: []git.Option{git.Flag{Name: "--format=format:%H"}}})
	require.NoError(b, err)

	logScanner := bufio.NewScanner(logCmd)

	var revisions []string
	for logScanner.Scan() {
		revisions = append(revisions, logScanner.Text())
	}

	require.NoError(b, logCmd.Wait())

	for i := 0; i < b.N; i++ {
		revision := revisions[b.N%len(revisions)]
		if withCache {
			md := metadata.New(map[string]string{
				"gitaly-session-id": "abc123",
			})

			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		_, err := client.FindCommit(ctx, &gitalypb.FindCommitRequest{
			Repository: repo,
			Revision:   []byte(revision),
		})
		require.NoError(b, err)
	}
}

func TestFindCommitWithCache(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, _, client := setupCommitServiceWithRepo(t, true)

	// get a list of revisions

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	logCmd, err := gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "log", Flags: []git.Option{git.Flag{Name: "--format=format:%H"}}})
	require.NoError(t, err)

	logScanner := bufio.NewScanner(logCmd)

	var revisions []string
	for logScanner.Scan() {
		revisions = append(revisions, logScanner.Text())
	}

	require.NoError(t, logCmd.Wait())

	for i := 0; i < 10; i++ {
		revision := revisions[i%len(revisions)]
		md := metadata.New(map[string]string{
			"gitaly-session-id": "abc123",
		})

		ctx = metadata.NewOutgoingContext(ctx, md)
		_, err := client.FindCommit(ctx, &gitalypb.FindCommitRequest{
			Repository: repo,
			Revision:   []byte(revision),
		})
		require.NoError(t, err)
	}
}
