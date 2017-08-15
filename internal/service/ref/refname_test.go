package ref

import (
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestFindRefNameSuccess(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client, conn := newRefClient(t)
	defer conn.Close()
	rpcRequest := &pb.FindRefNameRequest{
		Repository: testRepo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	response := string(c.GetName())

	if response != `refs/heads/expand-collapse-diffs` {
		t.Errorf("Expected FindRefName to return `refs/heads/expand-collapse-diffs`, got `%#v`", response)
	}
}

func TestFindRefNameEmptyCommit(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client, conn := newRefClient(t)
	defer conn.Close()
	rpcRequest := &pb.FindRefNameRequest{
		Repository: testRepo,
		CommitId:   "",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	if err == nil {
		t.Fatalf("Expected FindRefName to throw an error")
	}
	if grpc.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected FindRefName to throw InvalidArgument, got %v", err)
	}

	response := string(c.GetName())
	if response != `` {
		t.Errorf("Expected FindRefName to return empty-string, got %q", response)
	}
}

func TestFindRefNameInvalidRepo(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client, conn := newRefClient(t)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "fake", RelativePath: "path"}
	rpcRequest := &pb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	if err == nil {
		t.Fatalf("Expected FindRefName to throw an error")
	}
	if grpc.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected FindRefName to throw InvalidArgument, got %v", err)
	}

	response := string(c.GetName())
	if response != `` {
		t.Errorf("Expected FindRefName to return empty-string, got %q", response)
	}
}

func TestFindRefNameInvalidPrefix(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client, conn := newRefClient(t)
	defer conn.Close()
	rpcRequest := &pb.FindRefNameRequest{
		Repository: testRepo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/nonexistant/`),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	if err != nil {
		t.Fatalf("Expected FindRefName to not throw an error: %v", err)
	}
	if len(c.Name) > 0 {
		t.Errorf("Expected empty name, got %q instead", c.Name)
	}
}

func TestFindRefNameInvalidObject(t *testing.T) {
	server := runRefServer(t)
	defer server.Stop()

	client, conn := newRefClient(t)
	defer conn.Close()
	rpcRequest := &pb.FindRefNameRequest{
		Repository: testRepo,
		CommitId:   "dead1234dead1234dead1234dead1234dead1234",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	if err != nil {
		t.Fatalf("Expected FindRefName to not throw an error")
	}

	if len(c.GetName()) > 0 {
		t.Errorf("Expected FindRefName to return empty-string, got %q", string(c.GetName()))
	}
}
