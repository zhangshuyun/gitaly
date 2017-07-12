package commit

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCommitStatsSuccess(t *testing.T) {
	client := newCommitServiceClient(t)

	tests := []struct {
		revision  []byte
		additions int32
		deletions int32
		oid       string
	}{
		{revision: []byte("v1.1.0"), additions: 4, deletions: 0, oid: "5937ac0a7beb003549fc5fd26fc247adbce4a52e"},
		{revision: []byte("048721d90c449b244b7b4c53a9186b04330174ec"), additions: 4, deletions: 0, oid: "048721d90c449b244b7b4c53a9186b04330174ec"},
		{revision: []byte("874797c3a73b60d2187ed6e2fcabd289ff75171e"), additions: 21, deletions: 23, oid: "874797c3a73b60d2187ed6e2fcabd289ff75171e"},
		{revision: []byte("66eceea"), additions: 1, deletions: 1, oid: "66eceea0db202bb39c4e445e8ca28689645366c5"},
		{revision: []byte("improve/awesome"), additions: 4, deletions: 0, oid: "5937ac0a7beb003549fc5fd26fc247adbce4a52e"},
	}

	for _, test := range tests {
		t.Logf("test case: %q", test.revision)
		c, err := client.CommitStats(context.Background(), &pb.CommitStatsRequest{Repository: testRepo, Revision: test.revision})
		if err != nil {
			t.Fatal(err)
		}
		if len(test.oid) > 0 && strings.Compare(c.GetOid(), test.oid) != 0 {
			t.Errorf("OID different: %q != %q", c.GetOid(), test.oid)
		}

		if c.GetAdditions() != test.additions {
			t.Errorf("invalid number of additions: %d != %d", c.GetAdditions(), test.additions)
		}
		if c.GetDeletions() != test.deletions {
			t.Errorf("invalid number of deletions: %d != %d", c.GetDeletions(), test.deletions)
		}
	}
}

func TestCommitStatsInvalidArguments(t *testing.T) {
	client := newCommitServiceClient(t)

	tests := []struct {
		desc     string
		revision []byte
		repo     *pb.Repository
		code     codes.Code
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "empty repo", repo: &pb.Repository{}, code: codes.InvalidArgument},
		{desc: "invalid storage", repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "invalid revision", repo: testRepo, revision: []byte(""), code: codes.InvalidArgument},
	}

	for _, test := range tests {
		t.Logf("testing %q", test.desc)
		_, err := client.CommitStats(context.Background(), &pb.CommitStatsRequest{Repository: test.repo, Revision: test.revision})
		testhelper.AssertGrpcError(t, err, test.code, "")
	}
}
