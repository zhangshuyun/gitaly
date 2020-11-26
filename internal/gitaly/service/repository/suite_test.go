package repository

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type RepositoryServiceTestSuite struct {
	// This include a T() function to access *testing.T
	suite.Suite

	repository     *gitalypb.Repository
	repositoryPath string
	repoCleanupFn  func()

	srv *testhelper.TestServer

	client gitalypb.RepositoryServiceClient
	conn   *grpc.ClientConn
}

// Setup the repository service server and testing repository
// before each test
func (suite *RepositoryServiceTestSuite) SetupTest() {
	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(suite.T())

	suite.repository = testRepo
	suite.repositoryPath = repoPath
	suite.repoCleanupFn = cleanupFn

	suite.srv = buildTestingServer(suite.T(), config.Config, config.NewLocator(config.Config))

	req := suite.Require()
	req.NoError(suite.srv.Start())

	cl, conn := newRepositoryClient(suite.T(), "unix://"+suite.srv.Socket())
	suite.client = cl
	suite.conn = conn
}

func (suite *RepositoryServiceTestSuite) TearDownTest() {
	suite.conn.Close()
	suite.srv.Stop()

	suite.repoCleanupFn()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestRepositoryService(t *testing.T) {
	suite.Run(t, new(RepositoryServiceTestSuite))
}
