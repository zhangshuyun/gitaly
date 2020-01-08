package commit

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulGetCommitMessagesRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	message1 := strings.Repeat("a\n", helper.MaxCommitOrTagMessageSize*2)
	message2 := strings.Repeat("b\n", helper.MaxCommitOrTagMessageSize*2)

	commit1ID := testhelper.CreateCommit(t, testRepoPath, "local-big-commits", &testhelper.CreateCommitOpts{Message: message1})
	commit2ID := testhelper.CreateCommit(t, testRepoPath, "local-big-commits", &testhelper.CreateCommitOpts{Message: message2, ParentID: commit1ID})

	request := &gitalypb.GetCommitMessagesRequest{
		Repository: testRepo,
		CommitIds:  []string{commit1ID, commit2ID},
	}

	c, err := client.GetCommitMessages(ctx, request)
	require.NoError(t, err)

	expectedMessages := []*gitalypb.GetCommitMessagesResponse{
		{
			CommitId: commit1ID,
			Message:  []byte(message1),
		},
		{
			CommitId: commit2ID,
			Message:  []byte(message2),
		},
	}
	fetchedMessages := readAllMessagesFromClient(t, c)

	require.Equal(t, expectedMessages, fetchedMessages)
}

func TestFailedGetCommitMessagesRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc    string
		request *gitalypb.GetCommitMessagesRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.GetCommitMessagesRequest{
				Repository: nil,
				CommitIds:  []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e"},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.GetCommitMessages(ctx, testCase.request)
			require.NoError(t, err)

			for {
				_, err = c.Recv()
				if err != nil {
					break
				}
			}

			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func readAllMessagesFromClient(t *testing.T, c gitalypb.CommitService_GetCommitMessagesClient) (messages []*gitalypb.GetCommitMessagesResponse) {
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		if resp.CommitId != "" {
			messages = append(messages, resp)
			// first message contains a chunk of the message, so no need to append anything
			continue
		}

		currentMessage := messages[len(messages)-1]
		currentMessage.Message = append(currentMessage.Message, resp.Message...)
	}

	return
}
