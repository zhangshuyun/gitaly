package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/require"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func TestGetArchiveSuccess(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	formats := []pb.GetArchiveRequest_Format{
		pb.GetArchiveRequest_ZIP,
		pb.GetArchiveRequest_TAR,
		pb.GetArchiveRequest_TAR_GZ,
		pb.GetArchiveRequest_TAR_BZ2,
	}

	testCases := []struct {
		desc     string
		prefix   string
		commitID string
	}{
		{
			desc:     "without-prefix",
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			prefix:   "",
		},
		{
			desc:     "with-prefix",
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			prefix:   "my-prefix",
		},
	}

	for _, tc := range testCases {
		// Run test case with each format
		for _, format := range formats {
			testCaseName := fmt.Sprintf("%s-%s", tc.desc, format.String())
			t.Run(testCaseName, func(t *testing.T) {
				ctx, cancel := testhelper.Context()
				defer cancel()

				req := &pb.GetArchiveRequest{
					Repository: testRepo,
					CommitId:   tc.commitID,
					Prefix:     tc.prefix,
					Format:     format,
				}
				stream, err := client.GetArchive(ctx, req)
				require.NoError(t, err)

				data, err := consumeArchive(stream)
				require.NoError(t, err)

				archiveFile, err := ioutil.TempFile("", "")
				require.NoError(t, err)
				defer os.Remove(archiveFile.Name())

				_, err = archiveFile.Write(data)
				require.NoError(t, err)

				contents := string(compressedFileContents(t, format, archiveFile.Name()))

				require.Contains(t, contents, tc.prefix+"/.gitignore")
				require.Contains(t, contents, tc.prefix+"/LICENSE")
				require.Contains(t, contents, tc.prefix+"/README.md")
			})
		}
	}
}

func TestGetArchiveFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	commitID := "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"

	testCases := []struct {
		desc     string
		repo     *pb.Repository
		prefix   string
		commitID string
		format   pb.GetArchiveRequest_Format
		code     codes.Code
	}{
		{
			desc:     "Repository doesn't exist",
			repo:     &pb.Repository{StorageName: "fake", RelativePath: "path"},
			prefix:   "",
			commitID: commitID,
			format:   pb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "Repository is nil",
			repo:     nil,
			prefix:   "",
			commitID: commitID,
			format:   pb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "CommitId is empty",
			repo:     testRepo,
			prefix:   "",
			commitID: "",
			format:   pb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "Format is invalid",
			repo:     testRepo,
			prefix:   "",
			commitID: "",
			format:   pb.GetArchiveRequest_Format(-1),
			code:     codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &pb.GetArchiveRequest{
				Repository: tc.repo,
				CommitId:   tc.commitID,
				Prefix:     tc.prefix,
				Format:     tc.format,
			}
			stream, err := client.GetArchive(ctx, req)
			require.NoError(t, err)

			_, err = consumeArchive(stream)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func compressedFileContents(t *testing.T, format pb.GetArchiveRequest_Format, name string) []byte {
	switch format {
	case pb.GetArchiveRequest_TAR:
		return testhelper.MustRunCommand(t, nil, "tar", "tf", name)
	case pb.GetArchiveRequest_TAR_GZ:
		return testhelper.MustRunCommand(t, nil, "tar", "ztf", name)
	case pb.GetArchiveRequest_TAR_BZ2:
		return testhelper.MustRunCommand(t, nil, "tar", "jtf", name)
	case pb.GetArchiveRequest_ZIP:
		return testhelper.MustRunCommand(t, nil, "unzip", "-l", name)
	}

	return nil
}

func consumeArchive(stream pb.RepositoryService_GetArchiveClient) ([]byte, error) {
	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	})

	return ioutil.ReadAll(reader)
}
