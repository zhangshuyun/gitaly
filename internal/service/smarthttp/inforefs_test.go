package smarthttp

import (
	"io/ioutil"
	"strings"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	rpcRequest := &pb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsUploadPack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))
	if err != nil {
		t.Fatal(err)
	}

	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulInfoRefsReceivePack(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	rpcRequest := &pb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))
	if err != nil {
		t.Fatal(err)
	}

	assertGitRefAdvertisement(t, "InfoRefsReceivePack", string(response), "001f# service=git-receive-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"003e8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0",
	})
}

func TestFailureRepoNotFoundInfoRefsReceivePack(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: "testdata/data/another_repo"}
	rpcRequest := &pb.InfoRefsRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	for err == nil {
		_, err = c.Recv()
	}
	testhelper.AssertGrpcError(t, err, codes.NotFound, "not a git repository")
}

func TestFailureRepoNotSetInfoRefsReceivePack(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	rpcRequest := &pb.InfoRefsRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	for err == nil {
		_, err = c.Recv()
	}
	testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
}

func assertGitRefAdvertisement(t *testing.T, rpc, responseBody string, firstLine, lastLine string, middleLines []string) {
	responseLines := strings.Split(responseBody, "\n")

	if responseLines[0] != firstLine {
		t.Errorf("%q: expected response first line to be %q, found %q", rpc, firstLine, responseLines[0])
	}

	lastIndex := len(responseLines) - 1
	if responseLines[lastIndex] != lastLine {
		t.Errorf("%q: expected response last line to be %q, found %q", rpc, lastLine, responseLines[lastIndex])
	}

	for _, ref := range middleLines {
		if !strings.Contains(responseBody, ref) {
			t.Errorf("%q: expected response to contain %q, found none", rpc, ref)
		}
	}
}
