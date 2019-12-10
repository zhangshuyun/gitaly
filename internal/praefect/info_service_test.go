package praefect

import (
	"io"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

	"github.com/stretchr/testify/require"
)

func TestInfoService_ListRepositories(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	conf := testConfig(3)

	cleanup := CreateNodeStorages(t, conf.VirtualStorages[0].Nodes)
	defer cleanup()

	var primaryRepoPath string
	for _, node := range conf.VirtualStorages[0].Nodes {
		// we don't need to clean up the repos because the cleanup function from CreateNodeStorages
		// will clean up the entire temp dir
		_, destRepoPath, cleanup := cloneRepoAtStorage(t, testRepo, node.Storage)
		defer cleanup()
		if node.DefaultPrimary {
			primaryRepoPath = destRepoPath
		}
	}

	primaryRepo := *testRepo
	primaryRepo.StorageName = conf.VirtualStorages[0].Name

	ctx, cancel := testhelper.Context()
	defer cancel()

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	checksumResp, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{Repository: &primaryRepo})
	require.NoError(t, err)
	primaryChecksum := checksumResp.GetChecksum()

	infoClient := gitalypb.NewInfoServiceClient(cc)
	stream, err := infoClient.ListRepositories(ctx, &gitalypb.ListRepositoriesRequest{})
	require.NoError(t, err)

	responses := readListRepositoriesResponses(t, stream)

	require.Len(t, responses, 1)
	require.Equal(t, &gitalypb.ListRepositoriesResponse{
		Primary: &gitalypb.ListRepositoriesResponse_RepositoryDetails{
			Repository: &gitalypb.Repository{
				StorageName:  conf.VirtualStorages[0].Nodes[0].Storage,
				RelativePath: primaryRepo.GetRelativePath(),
			},
			Checksum: primaryChecksum,
		},
		Replicas: []*gitalypb.ListRepositoriesResponse_RepositoryDetails{
			{
				Repository: &gitalypb.Repository{
					StorageName:  conf.VirtualStorages[0].Nodes[1].Storage,
					RelativePath: primaryRepo.GetRelativePath(),
				},
				Checksum: primaryChecksum,
			},
			{
				Repository: &gitalypb.Repository{
					StorageName:  conf.VirtualStorages[0].Nodes[2].Storage,
					RelativePath: primaryRepo.GetRelativePath(),
				},
				Checksum: primaryChecksum,
			},
		},
	}, responses[0])

	// create a commit manually on the primary repo
	testhelper.CreateCommitOnNewBranch(t, primaryRepoPath)
	newChecksumResp, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{Repository: &primaryRepo})
	require.NoError(t, err)
	oldChecksum := primaryChecksum

	stream, err = infoClient.ListRepositories(ctx, &gitalypb.ListRepositoriesRequest{})
	require.NoError(t, err)

	responses = readListRepositoriesResponses(t, stream)

	require.Len(t, responses, 1)
	require.Equal(t, &gitalypb.ListRepositoriesResponse{
		Primary: &gitalypb.ListRepositoriesResponse_RepositoryDetails{
			Repository: &gitalypb.Repository{
				StorageName:  conf.VirtualStorages[0].Nodes[0].Storage,
				RelativePath: primaryRepo.GetRelativePath(),
			},
			Checksum: newChecksumResp.GetChecksum(),
		},
		Replicas: []*gitalypb.ListRepositoriesResponse_RepositoryDetails{
			{
				Repository: &gitalypb.Repository{
					StorageName:  conf.VirtualStorages[0].Nodes[1].Storage,
					RelativePath: primaryRepo.GetRelativePath(),
				},
				Checksum: oldChecksum,
			},
			{
				Repository: &gitalypb.Repository{
					StorageName:  conf.VirtualStorages[0].Nodes[2].Storage,
					RelativePath: primaryRepo.GetRelativePath(),
				},
				Checksum: oldChecksum,
			},
		},
	}, responses[0])
}

func readListRepositoriesResponses(t *testing.T, stream gitalypb.InfoService_ListRepositoriesClient) []*gitalypb.ListRepositoriesResponse {
	var responses []*gitalypb.ListRepositoriesResponse
	for {
		msg, err := stream.Recv()
		if err != nil {
			require.Error(t, io.EOF)
			break
		}
		responses = append(responses, msg)
	}

	return responses
}
