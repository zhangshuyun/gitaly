package ref

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes/timestamp"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/service/renameadapter"
)

var (
	serverSocketPath = testhelper.GetTemporaryGitalySocketFileName()
	testRepo         *pb.Repository
	testRepoPath     string
	localBranches    = map[string]*pb.GitCommit{
		"refs/heads/100%branch": {
			Id:      "1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
			Subject: []byte("Merge branch 'add-directory-with-space' into 'master'\r \r Add a directory containing a space in its name\r \r needed for verifying the fix of `https://gitlab.com/gitlab-com/support-forum/issues/952` \r \r See merge request !11"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Stan Hu"),
				Email: []byte("<stanhu@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1471558878},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Stan Hu"),
				Email: []byte("<stanhu@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1471558878},
			},
		},
		"refs/heads/improve/awesome": {
			Id:      "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			Subject: []byte("Add submodule from gitlab.com"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393491698},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393491698},
			},
		},
		"refs/heads/'test'": {
			Id:      "e56497bb5f03a90a51293fc6d516788730953899",
			Subject: []byte("Merge branch 'tree_helper_spec' into 'master'"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Sytse Sijbrandij"),
				Email: []byte("<sytse@gitlab.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1420925009},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Sytse Sijbrandij"),
				Email: []byte("<sytse@gitlab.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1420925009},
			},
		},
	}
)

func TestMain(m *testing.M) {
	var err error

	testRepo = testhelper.TestRepository()
	testRepoPath, err = helper.GetRepoPath(testRepo)
	if err != nil {
		log.Fatal(err)
	}

	// Use 100 bytes as the maximum message size to test that fragmenting the
	// ref list works correctly
	lines.MaxMsgSize = 100

	os.Exit(func() int {
		return m.Run()
	}())
}

func runRefServer(t *testing.T) *grpc.Server {
	os.Remove(serverSocketPath)
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterRefServer(grpcServer, renameadapter.NewRefAdapter(&server{}))
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer
}

func runRefServiceServer(t *testing.T) *grpc.Server {
	os.Remove(serverSocketPath)
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterRefServiceServer(grpcServer, &server{})
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer
}

func newRefClient(t *testing.T) (pb.RefClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewRefClient(conn), conn
}

func newRefServiceClient(t *testing.T) (pb.RefServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewRefServiceClient(conn), conn
}

func assertContainsLocalBranch(t *testing.T, branches []*pb.FindLocalBranchResponse, branch *pb.FindLocalBranchResponse) {
	for _, b := range branches {
		if bytes.Equal(branch.Name, b.Name) {
			if !testhelper.FindLocalBranchResponsesEqual(branch, b) {
				t.Errorf("Expected branch\n%v\ngot\n%v", branch, b)
			}
			return // Found the branch and it maches. Success!
		}
	}
	t.Errorf("Expected to find branch %q in local branches", branch.Name)
}

func assertContainsBranch(t *testing.T, branches []*pb.FindAllBranchesResponse_Branch, branch *pb.FindAllBranchesResponse_Branch) {
	var branchNames [][]byte

	for _, b := range branches {
		if bytes.Equal(branch.Name, b.Name) {
			if !testhelper.CommitsEqual(branch.Target, b.Target) {
				t.Errorf("Expected branch\n%v\ngot\n%v", branch, b)
			}
			return // Found the branch and it maches. Success!
		}
		branchNames = append(branchNames, b.Name)
	}

	t.Errorf("Expected to find branch %q in branches %s", branch.Name, branchNames)
}
