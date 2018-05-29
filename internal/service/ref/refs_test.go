package ref

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
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
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &pb.FindAllBranchNamesRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllBranchNames(ctx, rpcRequest)
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
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &pb.FindAllBranchNamesRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllBranchNames(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if helper.GrpcCode(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestInvalidRepoFindAllBranchNamesRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: "made/up/path"}
	rpcRequest := &pb.FindAllBranchNamesRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllBranchNames(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if helper.GrpcCode(recvError) != codes.NotFound {
		t.Fatal(recvError)
	}
}

func TestSuccessfulFindAllTagNames(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &pb.FindAllTagNamesRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllTagNames(ctx, rpcRequest)
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
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &pb.FindAllTagNamesRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllTagNames(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if helper.GrpcCode(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func TestInvalidRepoFindAllTagNamesRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: "made/up/path"}
	rpcRequest := &pb.FindAllTagNamesRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllTagNames(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if helper.GrpcCode(recvError) != codes.NotFound {
		t.Fatal(recvError)
	}
}

func TestHeadReference(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	headRef, err := headReference(ctx, testRepo)
	if err != nil {
		t.Fatal(err)
	}
	if string(headRef) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(headRef), "'")
	}
}

func TestHeadReferenceWithNonExistingHead(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Write bad HEAD
	ioutil.WriteFile(testRepoPath+"/HEAD", []byte("ref: refs/heads/nonexisting"), 0644)
	defer func() {
		// Restore HEAD
		ioutil.WriteFile(testRepoPath+"/HEAD", []byte("ref: refs/heads/master"), 0644)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	headRef, err := headReference(ctx, testRepo)
	if err != nil {
		t.Fatal(err)
	}
	if headRef != nil {
		t.Fatal("Expected HEAD reference to be nil, got '", string(headRef), "'")
	}
}

func TestDefaultBranchName(t *testing.T) {
	// We are going to override these functions during this test. Restore them after we're done
	defer func() {
		FindBranchNames = _findBranchNames
		headReference = _headReference
	}()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc            string
		findBranchNames func(context.Context, *pb.Repository) ([][]byte, error)
		headReference   func(context.Context, *pb.Repository) ([]byte, error)
		expected        []byte
	}{
		{
			desc:     "Get first branch when only one branch exists",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(context.Context, *pb.Repository) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo")}, nil
			},
			headReference: func(context.Context, *pb.Repository) ([]byte, error) { return nil, nil },
		},
		{
			desc:            "Get empy ref if no branches exists",
			expected:        nil,
			findBranchNames: func(context.Context, *pb.Repository) ([][]byte, error) { return [][]byte{}, nil },
			headReference:   func(context.Context, *pb.Repository) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the head reference when more than one branch exists",
			expected: []byte("refs/heads/bar"),
			findBranchNames: func(context.Context, *pb.Repository) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(context.Context, *pb.Repository) ([]byte, error) { return []byte("refs/heads/bar"), nil },
		},
		{
			desc:     "Get `ref/heads/master` when several branches exist",
			expected: []byte("refs/heads/master"),
			findBranchNames: func(context.Context, *pb.Repository) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/master"), []byte("refs/heads/bar")}, nil
			},
			headReference: func(context.Context, *pb.Repository) ([]byte, error) { return nil, nil },
		},
		{
			desc:     "Get the name of the first branch when several branches exists and no other conditions are met",
			expected: []byte("refs/heads/foo"),
			findBranchNames: func(context.Context, *pb.Repository) ([][]byte, error) {
				return [][]byte{[]byte("refs/heads/foo"), []byte("refs/heads/bar"), []byte("refs/heads/baz")}, nil
			},
			headReference: func(context.Context, *pb.Repository) ([]byte, error) { return nil, nil },
		},
	}

	for _, testCase := range testCases {
		FindBranchNames = testCase.findBranchNames
		headReference = testCase.headReference

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defaultBranch, err := DefaultBranchName(ctx, testRepo)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(defaultBranch, testCase.expected) {
			t.Fatalf("%s: expected %s, got %s instead", testCase.desc, testCase.expected, defaultBranch)
		}
	}
}

func TestSuccessfulFindDefaultBranchName(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &pb.FindDefaultBranchNameRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := client.FindDefaultBranchName(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	if name := r.GetName(); string(name) != "refs/heads/master" {
		t.Fatal("Expected HEAD reference to be 'ref/heads/master', got '", string(name), "'")
	}
}

func TestEmptyFindDefaultBranchNameRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &pb.FindDefaultBranchNameRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.FindDefaultBranchName(ctx, rpcRequest)

	if helper.GrpcCode(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestInvalidRepoFindDefaultBranchNameRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: "/made/up/path"}
	rpcRequest := &pb.FindDefaultBranchNameRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := client.FindDefaultBranchName(ctx, rpcRequest)

	if helper.GrpcCode(err) != codes.NotFound {
		t.Fatal(err)
	}
}

func TestSuccessfulFindAllTagsRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	testRepoCopy, testRepoCopyPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	blobID := "faaf198af3a36dbf41961466703cc1d47c61d051"
	commitID := "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"

	gitCommit := &pb.GitCommit{
		Id:      commitID,
		Subject: []byte("More submodules"),
		Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
		Author: &pb.CommitAuthor{
			Name:  []byte("Dmitriy Zaporozhets"),
			Email: []byte("dmitriy.zaporozhets@gmail.com"),
			Date:  &timestamp.Timestamp{Seconds: 1393491261},
		},
		Committer: &pb.CommitAuthor{
			Name:  []byte("Dmitriy Zaporozhets"),
			Email: []byte("dmitriy.zaporozhets@gmail.com"),
			Date:  &timestamp.Timestamp{Seconds: 1393491261},
		},
		ParentIds: []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
		BodySize:  84,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	bigCommitID := testhelper.CreateCommit(t, testRepoCopyPath, "local-big-commits", &testhelper.CreateCommitOpts{
		Message:  "An empty commit with REALLY BIG message\n\n" + strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1),
		ParentID: "60ecb67744cb56576c30214ff52294f8ce2def98",
	})
	bigCommit, err := log.GetCommit(ctx, testRepoCopy, bigCommitID, "")
	require.NoError(t, err)

	annotatedTagID := testhelper.CreateTag(t, testRepoCopyPath, "v1.2.0", blobID, &testhelper.CreateTagOpts{Message: "Blob tag"})

	testhelper.CreateTag(t, testRepoCopyPath, "v1.3.0", commitID, nil)
	testhelper.CreateTag(t, testRepoCopyPath, "v1.4.0", blobID, nil)

	// To test recursive resolving to a commit
	testhelper.CreateTag(t, testRepoCopyPath, "v1.5.0", "v1.3.0", nil)

	// A tag to commit with a big message
	testhelper.CreateTag(t, testRepoCopyPath, "v1.6.0", bigCommitID, nil)

	// A tag with a big message
	bigMessage := strings.Repeat("a", 11*1024)
	bigMessageTag1ID := testhelper.CreateTag(t, testRepoCopyPath, "v1.7.0", commitID, &testhelper.CreateTagOpts{Message: bigMessage})

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	rpcRequest := &pb.FindAllTagsRequest{Repository: testRepoCopy}

	c, err := client.FindAllTags(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var receivedTags []*pb.Tag
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		receivedTags = append(receivedTags, r.GetTags()...)
	}

	expectedTags := []*pb.Tag{
		{
			Name:         []byte("v1.0.0"),
			Id:           "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8",
			TargetCommit: gitCommit,
			Message:      []byte("Release"),
			MessageSize:  7,
		},
		{
			Name: []byte("v1.1.0"),
			Id:   "8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b",
			TargetCommit: &pb.GitCommit{
				Id:      "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
				Subject: []byte("Add submodule from gitlab.com"),
				Body:    []byte("Add submodule from gitlab.com\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491698},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491698},
				},
				ParentIds: []string{"570e7b2abdd848b95f2f578043fc23bd6f6fd24d"},
				BodySize:  98,
			},
			Message:     []byte("Version 1.1.0"),
			MessageSize: 13,
		},
		{
			Name:        []byte("v1.2.0"),
			Id:          string(annotatedTagID),
			Message:     []byte("Blob tag"),
			MessageSize: 8,
		},
		{
			Name:         []byte("v1.3.0"),
			Id:           string(commitID),
			TargetCommit: gitCommit,
		},
		{
			Name: []byte("v1.4.0"),
			Id:   string(blobID),
		},
		{
			Name:         []byte("v1.5.0"),
			Id:           string(commitID),
			TargetCommit: gitCommit,
		},
		{
			Name:         []byte("v1.6.0"),
			Id:           string(bigCommitID),
			TargetCommit: bigCommit,
		},
		{
			Name:         []byte("v1.7.0"),
			Id:           string(bigMessageTag1ID),
			Message:      []byte(bigMessage[:helper.MaxCommitOrTagMessageSize]),
			MessageSize:  int64(len(bigMessage)),
			TargetCommit: gitCommit,
		},
	}

	if len(receivedTags) < len(expectedTags) {
		t.Fatalf("expected at least %d tags, got %d", len(expectedTags), len(receivedTags))
	}

	for _, expectedTag := range expectedTags {
		t.Run(string(expectedTag.Name), func(t *testing.T) {
			receivedTag := findTag(receivedTags, expectedTag.Name)
			require.NotNil(t, receivedTag, "tag not found")
			require.Equal(t, expectedTag, receivedTag)
		})
	}
}

func findTag(tags []*pb.Tag, tagName []byte) *pb.Tag {
	for _, t := range tags {
		if bytes.Equal(t.Name, tagName) {
			return t
		}
	}
	return nil
}

func TestInvalidFindAllTagsRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	testCases := []struct {
		desc    string
		request *pb.FindAllTagsRequest
	}{
		{
			desc:    "empty request",
			request: &pb.FindAllTagsRequest{},
		},
		{
			desc: "invalid repo",
			request: &pb.FindAllTagsRequest{
				Repository: &pb.Repository{
					StorageName:  "fake",
					RelativePath: "repo",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.FindAllTags(ctx, tc.request)
			if err != nil {
				t.Fatal(err)
			}

			var recvError error
			for recvError == nil {
				_, recvError = c.Recv()
			}

			testhelper.RequireGrpcError(t, recvError, codes.InvalidArgument)
		})
	}
}

func TestSuccessfulFindLocalBranches(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &pb.FindLocalBranchesRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindLocalBranches(ctx, rpcRequest)
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

	for name, target := range localBranches {
		localBranch := &pb.FindLocalBranchResponse{
			Name:          []byte(name),
			CommitId:      target.Id,
			CommitSubject: target.Subject,
			CommitAuthor: &pb.FindLocalBranchCommitAuthor{
				Name:  target.Author.Name,
				Email: target.Author.Email,
				Date:  target.Author.Date,
			},
			CommitCommitter: &pb.FindLocalBranchCommitAuthor{
				Name:  target.Committer.Name,
				Email: target.Committer.Email,
				Date:  target.Committer.Date,
			},
		}
		assertContainsLocalBranch(t, branches, localBranch)
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

	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			rpcRequest := &pb.FindLocalBranchesRequest{Repository: testRepo, SortBy: testCase.sortBy}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.FindLocalBranches(ctx, rpcRequest)
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
		})
	}
}

func TestEmptyFindLocalBranchesRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &pb.FindLocalBranchesRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindLocalBranches(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	var recvError error
	for recvError == nil {
		_, recvError = c.Recv()
	}

	if helper.GrpcCode(recvError) != codes.InvalidArgument {
		t.Fatal(recvError)
	}
}

func createRemoteBranch(t *testing.T, repoPath, remoteName, branchName, ref string) {
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref",
		"refs/remotes/"+remoteName+"/"+branchName, ref)
}

func TestSuccessfulFindAllBranchesRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	remoteBranch := &pb.FindAllBranchesResponse_Branch{
		Name: []byte("refs/remotes/origin/fake-remote-branch"),
		Target: &pb.GitCommit{
			Id:      "913c66a37b4a45b9769037c55c2d238bd0942d2e",
			Subject: []byte("Files, encoding and much more"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393488896},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Dmitriy Zaporozhets"),
				Email: []byte("<dmitriy.zaporozhets@gmail.com>"),
				Date:  &timestamp.Timestamp{Seconds: 1393488896},
			},
		},
	}

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	createRemoteBranch(t, testRepoPath, "origin", "fake-remote-branch",
		remoteBranch.Target.Id)

	request := &pb.FindAllBranchesRequest{Repository: testRepo}
	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindAllBranches(ctx, request)
	if err != nil {
		t.Fatal(err)
	}

	branches := readFindAllBranchesResponsesFromClient(t, c)

	// It contains local branches
	for name, target := range localBranches {
		branch := &pb.FindAllBranchesResponse_Branch{
			Name:   []byte(name),
			Target: target,
		}
		assertContainsBranch(t, branches, branch)
	}

	// It contains our fake remote branch
	assertContainsBranch(t, branches, remoteBranch)
}

func TestSuccessfulFindAllBranchesRequestWithMergedBranches(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	localRefs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--format=%(refname:strip=2)", "refs/heads")
	for _, ref := range strings.Split(string(localRefs), "\n") {
		ref = strings.TrimSpace(ref)
		if _, ok := localBranches["refs/heads/"+ref]; ok || ref == "master" || ref == "" {
			continue
		}
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-D", ref)
	}

	expectedRefs := []string{"refs/heads/100%branch", "refs/heads/improve/awesome", "refs/heads/'test'"}

	var expectedBranches []*pb.FindAllBranchesResponse_Branch
	for _, name := range expectedRefs {
		target, ok := localBranches[name]
		require.True(t, ok)

		branch := &pb.FindAllBranchesResponse_Branch{
			Name:   []byte(name),
			Target: target,
		}
		expectedBranches = append(expectedBranches, branch)
	}

	masterCommit, err := log.GetCommit(ctx, testRepo, "master", "")
	require.NoError(t, err)
	expectedBranches = append(expectedBranches, &pb.FindAllBranchesResponse_Branch{
		Name:   []byte("refs/heads/master"),
		Target: masterCommit,
	})

	testCases := []struct {
		desc             string
		request          *pb.FindAllBranchesRequest
		expectedBranches []*pb.FindAllBranchesResponse_Branch
	}{
		{
			desc: "all merged branches",
			request: &pb.FindAllBranchesRequest{
				Repository: testRepo,
				MergedOnly: true,
			},
			expectedBranches: expectedBranches,
		},
		{
			desc: "all merged from a list of branches",
			request: &pb.FindAllBranchesRequest{
				Repository: testRepo,
				MergedOnly: true,
				MergedBranches: [][]byte{
					[]byte("refs/heads/100%branch"),
					[]byte("refs/heads/improve/awesome"),
					[]byte("refs/heads/gitaly-stuff"),
				},
			},
			expectedBranches: expectedBranches[:2],
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			c, err := client.FindAllBranches(ctx, testCase.request)
			require.NoError(t, err)

			branches := readFindAllBranchesResponsesFromClient(t, c)
			require.Len(t, branches, len(testCase.expectedBranches))

			for _, branch := range branches {
				// The GitCommit object returned by GetCommit() above and the one returned in the response
				// vary a lot. We can't guarantee that master will be fixed at a certain commit so we can't create
				// a structure for it manually, hence this hack.
				if string(branch.Name) == "refs/heads/master" {
					continue
				}

				assertContainsBranch(t, testCase.expectedBranches, branch)
			}
		})
	}
}

func TestInvalidFindAllBranchesRequest(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	testCases := []struct {
		description string
		request     pb.FindAllBranchesRequest
	}{
		{
			description: "Empty request",
			request:     pb.FindAllBranchesRequest{},
		},
		{
			description: "Invalid repo",
			request: pb.FindAllBranchesRequest{
				Repository: &pb.Repository{
					StorageName:  "fake",
					RelativePath: "repo",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.FindAllBranches(ctx, &tc.request)
			if err != nil {
				t.Fatal(err)
			}

			var recvError error
			for recvError == nil {
				_, recvError = c.Recv()
			}

			testhelper.RequireGrpcError(t, recvError, codes.InvalidArgument)
		})
	}
}

func readFindAllBranchesResponsesFromClient(t *testing.T, c pb.RefService_FindAllBranchesClient) (branches []*pb.FindAllBranchesResponse_Branch) {
	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		branches = append(branches, r.GetBranches()...)
	}

	return
}

func TestListTagNamesContainingCommit(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		description string
		commitID    string
		code        codes.Code
		limit       uint32
		tags        []string
	}{
		{
			description: "no commit ID",
			commitID:    "",
			code:        codes.InvalidArgument,
		},
		{
			description: "current master HEAD",
			commitID:    "e63f41fe459e62e1228fcef60d7189127aeba95a",
			code:        codes.OK,
			tags:        []string{},
		},
		{
			description: "init commit",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			tags:        []string{"v1.0.0", "v1.1.0"},
		},
		{
			description: "limited response size",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			limit:       1,
			tags:        []string{"v1.0.0"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &pb.ListTagNamesContainingCommitRequest{Repository: testRepo, CommitId: tc.commitID}

			c, err := client.ListTagNamesContainingCommit(ctx, request)
			require.NoError(t, err)

			var names []string
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				} else if tc.code != codes.OK {
					testhelper.RequireGrpcError(t, err, tc.code)

					return
				}
				require.NoError(t, err)

				for _, name := range r.GetTagNames() {
					names = append(names, string(name))
				}
			}

			// Test for inclusion instead of equality because new refs
			// will get added to the gitlab-test repo over time.
			require.Subset(t, names, tc.tags)
		})
	}
}

func TestListBranchNamesContainingCommit(t *testing.T) {
	server, serverSocketPath := runRefServiceServer(t)
	defer server.Stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		description string
		commitID    string
		code        codes.Code
		limit       uint32
		branches    []string
	}{
		{
			description: "no commit ID",
			commitID:    "",
			code:        codes.InvalidArgument,
		},
		{
			description: "current master HEAD",
			commitID:    "e63f41fe459e62e1228fcef60d7189127aeba95a",
			code:        codes.OK,
			branches:    []string{"master"},
		},
		{
			// gitlab-test contains a branch refs/heads/1942eed5cc108b19c7405106e81fa96125d0be22
			// which is in conflift with a commit with the same ID
			description: "branch name is also commit id",
			commitID:    "1942eed5cc108b19c7405106e81fa96125d0be22",
			code:        codes.OK,
			branches:    []string{"1942eed5cc108b19c7405106e81fa96125d0be22"},
		},
		{
			description: "init commit",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			branches: []string{
				"deleted-image-test",
				"ends-with.json",
				"master",
				"conflict-non-utf8",
				"'test'",
				"ʕ•ᴥ•ʔ",
				"'test'",
				"100%branch",
			},
		},
		{
			description: "init commit",
			commitID:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			code:        codes.OK,
			limit:       1,
			branches:    []string{"'test'"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			request := &pb.ListBranchNamesContainingCommitRequest{Repository: testRepo, CommitId: tc.commitID}

			c, err := client.ListBranchNamesContainingCommit(ctx, request)
			require.NoError(t, err)

			var names []string
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				} else if tc.code != codes.OK {
					testhelper.RequireGrpcError(t, err, tc.code)

					return
				}
				require.NoError(t, err)

				for _, name := range r.GetBranchNames() {
					names = append(names, string(name))
				}
			}

			// Test for inclusion instead of equality because new refs
			// will get added to the gitlab-test repo over time.
			require.Subset(t, names, tc.branches)
		})
	}
}
