package praefect

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	gconfig "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func TestConsistencyCheck(t *testing.T) {
	oldStorages := gconfig.Config.Storages
	defer func() { gconfig.Config.Storages = oldStorages }()

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					0: {
						Storage: "gitaly-0",
						Address: "tcp::/this-doesnt-matter",
					},
					1: {
						Storage: "gitaly-1",
						Address: "tcp::/this-doesnt-matter",
					},
				},
			},
		},
	}

	virtualStorage := conf.VirtualStorages[0]
	primary := virtualStorage.Nodes[0]
	secondary := virtualStorage.Nodes[1]

	testStorages := []gconfig.Storage{
		{
			Name: virtualStorage.Nodes[0].Storage,
			Path: tempStoragePath(t),
		},
		{
			Name: virtualStorage.Nodes[1].Storage,
			Path: tempStoragePath(t),
		},
	}

	gconfig.Config.Storages = append(gconfig.Config.Storages, testStorages...)
	defer func() {
		for _, ts := range testStorages {
			require.NoError(t, os.RemoveAll(ts.Path))
		}
	}()

	repo0, _, cleanup0 := testhelper.NewTestRepo(t)
	defer cleanup0()

	_, _, cleanupReference := cloneRepoAtStorage(t, repo0, virtualStorage.Nodes[0].Storage)
	defer cleanupReference()

	_, targetRepoPath, cleanupTarget := cloneRepoAtStorage(t, repo0, virtualStorage.Nodes[1].Storage)
	defer cleanupTarget()

	cc, _, cleanup := runPraefectServerWithGitaly(t, conf)
	defer cleanup()

	praefectCli := gitalypb.NewPraefectInfoServiceClient(cc)

	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(10 * time.Second))
	defer cancel()

	disableReconcilliation := true

	requestConsistencyCheck := func() *gitalypb.ConsistencyCheckResponse {
		stream, err := praefectCli.ConsistencyCheck(ctx, &gitalypb.ConsistencyCheckRequest{
			VirtualStorage:         virtualStorage.Name,
			ReferenceStorage:       primary.Storage,
			TargetStorage:          secondary.Storage,
			DisableReconcilliation: disableReconcilliation,
		})
		require.NoError(t, err)

		responses := consumeConsistencyCheckResponses(t, stream)
		require.Len(t, responses, 1)

		resp := responses[0]

		require.Equal(t, repo0.RelativePath, resp.RepoRelativePath)
		require.Equal(t, primary.Storage, resp.ReferenceStorage)

		return resp
	}

	resp := requestConsistencyCheck()
	require.Equal(t, resp.ReferenceChecksum, resp.TargetChecksum,
		"both repos expected to be consistent after initial clone")
	require.Zero(t, resp.ReplJobId)

	testhelper.MustRunCommand(t, nil, "git", "-C", targetRepoPath, "update-ref", "HEAD", "spooky-stuff")

	resp = requestConsistencyCheck()
	require.NotEqual(t, resp.ReferenceChecksum, resp.TargetChecksum,
		"repos should no longer be consistent after target HEAD changed")
	require.Zero(t, resp.ReplJobId)

	disableReconcilliation = false
	resp = requestConsistencyCheck()
	require.NotZero(t, resp.ReplJobId)
}

func consumeConsistencyCheckResponses(t *testing.T, stream gitalypb.PraefectInfoService_ConsistencyCheckClient) []*gitalypb.ConsistencyCheckResponse {
	var responses []*gitalypb.ConsistencyCheckResponse
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		responses = append(responses, resp)
	}
	return responses
}
