package ref

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func containsRef(refs [][]byte, ref string) bool {
	for _, b := range refs {
		if string(b) == ref {
			return true
		}
	}
	return false
}

func TestSuccessfulFindAllBranchNames(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rpcRequest := &pb.FindAllBranchNamesRequest{Repository: repo}

	c, err := client.FindAllBranchNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var names [][]byte
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, r.GetNames()...)
	}
	for _, branch := range []string{"master", "100%branch", "improve/awesome", "'test'"} {
		if !containsRef(names, "refs/heads/"+branch) {
			t.Fatalf("Expected to find branch %q in all branch names", branch)
		}
	}
}

func TestEmptyFindAllBranchNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindAllBranchNamesRequest{}

	c, err := client.FindAllBranchNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestInvalidRepoFindAllBranchNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: "/made/up/path"}
	rpcRequest := &pb.FindAllBranchNamesRequest{Repository: repo}

	c, err := client.FindAllBranchNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.NotFound {
		t.Fatal(recvError)
	}
}

func TestSuccessfulFindAllTagNames(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rpcRequest := &pb.FindAllTagNamesRequest{Repository: repo}

	c, err := client.FindAllTagNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var names [][]byte
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, r.GetNames()...)
	}

	for _, tag := range []string{"v1.0.0", "v1.1.0"} {
		if !containsRef(names, "refs/tags/"+tag) {
			t.Fatal("Expected to find tag", tag, "in all tag names")
		}
	}
}

func TestEmptyFindAllTagNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindAllTagNamesRequest{}

	c, err := client.FindAllTagNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestInvalidRepoFindAllTagNamesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: "/made/up/path"}
	rpcRequest := &pb.FindAllTagNamesRequest{Repository: repo}

	c, err := client.FindAllTagNames(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.NotFound {
		t.Fatal(recvError)
	}
}

func TestHeadReference(t *testing.T) {
	headRef, err := headReference(context.Background(), testRepoPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(headRef) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(headRef), "'")
	}
}

func TestDefaultBranchName(t *testing.T) {
	// We are going to override these functions during this test. Restore them after we're done
	defer func() {
		findBranchNames = _findBranchNames
		headReference = _headReference
	}()

	testCases := []struct {
		desc            string
		findBranchNames func(context.Context, string) ([][]byte, error)
		headReference   func(context.Context, string) ([]byte, error)
		expected        []byte
	}{
		{
			desc:     "Get first branch when only one branch exists",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(context.Context, string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo")}, nil
			},
			headReference: func(context.Context, string) ([]byte, error) { return nil, nil },
		},
		{
			desc:            "Get empy ref if no branches exists",
			expected:        nil,
			findBranchNames: func(context.Context, string) ([][]byte, error) { return [][]byte{}, nil },
			headReference:   func(context.Context, string) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the head reference when more than one branch exists",
			expected: []byte("refs/heads/bar"),
			findBranchNames: func(context.Context, string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(context.Context, string) ([]byte, error) { return []byte("refs/heads/bar"), nil },
		},
		{
			desc:     "Get `ref/heads/master` when several branches exist",
			expected: []byte("refs/heads/master"),
			findBranchNames: func(context.Context, string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/master"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(context.Context, string) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the first branch when several branches exists and no other conditions are met",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(context.Context, string) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar"), []byte("refs/heads/baz")}, nil
			},
			headReference: func(context.Context, string) ([]byte, error) { return nil, nil },
		},
	}

	for _, testCase := range testCases {
		findBranchNames = testCase.findBranchNames
		headReference = testCase.headReference

		defaultBranch, err := defaultBranchName(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(defaultBranch, testCase.expected) {
			t.Fatalf("%s: expected %s, got %s instead", testCase.desc, testCase.expected, defaultBranch)
		}
	}
}

func TestSuccessfulFindDefaultBranchName(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rpcRequest := &pb.FindDefaultBranchNameRequest{Repository: repo}

	r, err := client.FindDefaultBranchName(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	if name := r.GetName(); string(name) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(name), "'")
	}
}

func TestEmptyFindDefaultBranchNameRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindDefaultBranchNameRequest{}

	_, err := client.FindDefaultBranchName(context.Background(), rpcRequest)

	if grpc.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestInvalidRepoFindDefaultBranchNameRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: "/made/up/path"}
	rpcRequest := &pb.FindDefaultBranchNameRequest{Repository: repo}

	_, err := client.FindDefaultBranchName(context.Background(), rpcRequest)

	if grpc.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
}

func localBranches() []*pb.FindLocalBranchResponse {
	return []*pb.FindLocalBranchResponse{
		{
			Name:          []byte("refs/heads/100%branch"),
			CommitId:      "1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
			CommitSubject: []byte("Merge branch 'add-directory-with-space' into 'master'\r \r Add a directory containing a space in its name\r \r needed for verifying the fix of `https://gitlab.com/gitlab-com/support-forum/issues/952` \r \r See merge request !11"),
			CommitAuthor: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Stan Hu"),
				Email: []byte("<stanhu@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1471558878},
			},
			CommitCommitter: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Stan Hu"),
				Email: []byte("<stanhu@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1471558878},
			},
		},
		{
			Name:          []byte("refs/heads/improve/awesome"),
			CommitId:      "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			CommitSubject: []byte("Add submodule from gitlab.com"),
			CommitAuthor: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393491698},
			},
			CommitCommitter: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393491698},
			},
		},
		{
			Name:          []byte("refs/heads/'test'"),
			CommitId:      "e56497bb5f03a90a51293fc6d516788730953899",
			CommitSubject: []byte("Merge branch 'tree_helper_spec' into 'master'"),
			CommitAuthor: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Sytse Sijbrandij"),
				Email: []byte("<sytse@gitlab.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1420925009},
			},
			CommitCommitter: &pb.FindLocalBranchCommitAuthor{
				Name:  []byte("Sytse Sijbrandij"),
				Email: []byte("<sytse@gitlab.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1420925009},
			},
		},
	}
}

func authorsEqual(a *pb.FindLocalBranchCommitAuthor, b *pb.FindLocalBranchCommitAuthor) bool {
	return bytes.Equal(a.Name, b.Name) &&
		bytes.Equal(a.Email, b.Email) &&
		a.Date.Seconds == b.Date.Seconds
}

func branchesEqual(a *pb.FindLocalBranchResponse, b *pb.FindLocalBranchResponse) bool {
	return a.CommitId == b.CommitId &&
		bytes.Equal(a.CommitSubject, b.CommitSubject) &&
		authorsEqual(a.CommitAuthor, b.CommitAuthor) &&
		authorsEqual(a.CommitCommitter, b.CommitCommitter)
}

func validateContainsBranch(t *testing.T, branches []*pb.FindLocalBranchResponse, branch *pb.FindLocalBranchResponse) {
	for _, b := range branches {
		if bytes.Equal(branch.Name, b.Name) {
			if !branchesEqual(branch, b) {
				t.Fatalf("Expected branch\n%v\ngot\n%v", branch, b)
			}
			return // Found the branch and it maches. Success!
		}
	}
	t.Fatalf("Expected to find branch %q in local branches", branch.Name)
}

func TestSuccessfulFindLocalBranches(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rpcRequest := &pb.FindLocalBranchesRequest{Repository: repo}

	c, err := client.FindLocalBranches(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var branches []*pb.FindLocalBranchResponse
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		branches = append(branches, r.GetBranches()...)
	}

	for _, branch := range localBranches() {
		validateContainsBranch(t, branches, branch)
	}
}

// Test that `s` contains the elements in `relativeOrder` in that order
// (relative to each other)
func isOrderedSubset(subset, set []string) bool {
	subsetIndex := 0 // The string we are currently looking for from `subset`
	for _, element := range set {
		if element != subset[subsetIndex] {
			continue
		}

		subsetIndex++

		if subsetIndex == len(subset) { // We found all elements in that order
			return true
		}
	}
	return false
}

func TestFindLocalBranchesSort(t *testing.T) {
	testCases := []struct {
		desc          string
		relativeOrder []string
		sortBy        pb.FindLocalBranchesRequest_SortBy
	}{
		{
			desc:          "In ascending order by name",
			relativeOrder: []string{"refs/heads/'test'", "refs/heads/100%branch", "refs/heads/improve/awesome", "refs/heads/master"},
			sortBy:        pb.FindLocalBranchesRequest_NAME,
		},
		{
			desc:          "In ascending order by commiter date",
			relativeOrder: []string{"refs/heads/improve/awesome", "refs/heads/'test'", "refs/heads/100%branch", "refs/heads/master"},
			sortBy:        pb.FindLocalBranchesRequest_UPDATED_ASC,
		},
		{
			desc:          "In descending order by commiter date",
			relativeOrder: []string{"refs/heads/master", "refs/heads/100%branch", "refs/heads/'test'", "refs/heads/improve/awesome"},
			sortBy:        pb.FindLocalBranchesRequest_UPDATED_DESC,
		},
	}

	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	repo := &pb.Repository{Path: testRepoPath}

	for _, testCase := range testCases {
		rpcRequest := &pb.FindLocalBranchesRequest{Repository: repo, SortBy: testCase.sortBy}

		c, err := client.FindLocalBranches(context.Background(), rpcRequest)
		if err != nil {
			t.Fatal(err)
		}

		var branches []string
		for {
			r, err := c.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, branch := range r.GetBranches() {
				branches = append(branches, string(branch.Name))
			}
		}

		if !isOrderedSubset(testCase.relativeOrder, branches) {
			t.Fatalf("%s: Expected branches to have relative order %v; got them as %v", testCase.desc, testCase.relativeOrder, branches)
		}
	}
}

func TestEmptyFindLocalBranchesRequest(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client := newRefClient(t)
	rpcRequest := &pb.FindLocalBranchesRequest{}

	c, err := client.FindLocalBranches(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if grpc.Code(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}
