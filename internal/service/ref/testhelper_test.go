package ref

import (
	"bytes"
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	localBranches = map[string]*gitalypb.GitCommit{
		"refs/heads/100%branch": {
			Id:        "1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
			Body:      []byte("Merge branch 'add-directory-with-space' into 'master'\r\n\r\nAdd a directory containing a space in its name\r\n\r\nneeded for verifying the fix of `https://gitlab.com/gitlab-com/support-forum/issues/952` \r\n\r\nSee merge request !11"),
			BodySize:  221,
			ParentIds: []string{"6907208d755b60ebeacb2e9dfea74c92c3449a1f", "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e"},
			Subject:   []byte("Merge branch 'add-directory-with-space' into 'master'"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Stan Hu"),
				Email:    []byte("stanhu@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1471558878},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Stan Hu"),
				Email:    []byte("stanhu@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1471558878},
				Timezone: []byte("+0000"),
			},
		},
		"refs/heads/improve/awesome": {
			Id:        "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			Subject:   []byte("Add submodule from gitlab.com"),
			Body:      []byte("Add submodule from gitlab.com\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
			BodySize:  98,
			ParentIds: []string{"570e7b2abdd848b95f2f578043fc23bd6f6fd24d"},
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491698},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491698},
				Timezone: []byte("+0200"),
			},
			SignatureType: gitalypb.SignatureType_PGP,
		},
		"refs/heads/'test'": {
			Id:        "e56497bb5f03a90a51293fc6d516788730953899",
			Subject:   []byte("Merge branch 'tree_helper_spec' into 'master'"),
			Body:      []byte("Merge branch 'tree_helper_spec' into 'master'\n\nAdd directory structure for tree_helper spec\n\nThis directory structure is needed for a testing the method flatten_tree(tree) in the TreeHelper module\n\nSee [merge request #275](https://gitlab.com/gitlab-org/gitlab-ce/merge_requests/275#note_732774)\n\nSee merge request !2\n"),
			BodySize:  317,
			ParentIds: []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e", "4cd80ccab63c82b4bad16faa5193fbd2aa06df40"},
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Sytse Sijbrandij"),
				Email:    []byte("sytse@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1420925009},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Sytse Sijbrandij"),
				Email:    []byte("sytse@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1420925009},
				Timezone: []byte("+0000"),
			},
		},
	}
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	// Force small messages to test that fragmenting the
	// ref list works correctly
	lines.ItemsPerMessage = 3

	return m.Run()
}

func runRefServiceServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterRefServiceServer(srv.GrpcServer(), &server{})
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
}

func newRefServiceClient(t *testing.T, serverSocketPath string) (gitalypb.RefServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewRefServiceClient(conn), conn
}

func assertContainsLocalBranch(t *testing.T, branches []*gitalypb.FindLocalBranchResponse, branch *gitalypb.FindLocalBranchResponse) {
	for _, b := range branches {
		if bytes.Equal(branch.Name, b.Name) {
			if !testhelper.FindLocalBranchResponsesEqual(branch, b) {
				t.Errorf("Expected branch\n%v\ngot\n%v", branch, b)
			}
			require.Equal(t, branch.Commit, b.Commit)
			return // Found the branch and it maches. Success!
		}
	}
	t.Errorf("Expected to find branch %q in local branches", branch.Name)
}

func assertContainsBranch(t *testing.T, branches []*gitalypb.FindAllBranchesResponse_Branch, branch *gitalypb.FindAllBranchesResponse_Branch) {
	var branchNames [][]byte

	for _, b := range branches {
		if bytes.Equal(branch.Name, b.Name) {
			require.Equal(t, b.Target, branch.Target, "mismatched targets")
			return // Found the branch and it maches. Success!
		}
		branchNames = append(branchNames, b.Name)
	}

	t.Errorf("Expected to find branch %q in branches %s", branch.Name, branchNames)
}
