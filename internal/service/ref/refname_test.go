package ref

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRefNameSuccess(t *testing.T) {
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.FindRefNameRequest{
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
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.FindRefNameRequest{
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
	if helper.GrpcCode(err) != codes.InvalidArgument {
		t.Errorf("Expected FindRefName to throw InvalidArgument, got %v", err)
	}

	response := string(c.GetName())
	if response != `` {
		t.Errorf("Expected FindRefName to return empty-string, got %q", response)
	}
}

func TestFindRefNameInvalidRepo(t *testing.T) {
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()
	repo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}
	rpcRequest := &gitalypb.FindRefNameRequest{
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
	if helper.GrpcCode(err) != codes.InvalidArgument {
		t.Errorf("Expected FindRefName to throw InvalidArgument, got %v", err)
	}

	response := string(c.GetName())
	if response != `` {
		t.Errorf("Expected FindRefName to return empty-string, got %q", response)
	}
}

func TestFindRefNameInvalidPrefix(t *testing.T) {
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.FindRefNameRequest{
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
	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.FindRefNameRequest{
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

func TestFindRefCmd(t *testing.T) {
	testCases := []struct {
		desc         string
		cmd          ForEachRefCmd
		expectedErr  error
		expectedArgs []string
	}{
		{
			desc: "wrong command",
			cmd: ForEachRefCmd{
				SubCmd: git.SubCmd{
					Name: "rev-list",
				},
			},
			expectedErr: ErrOnlyForEachRefAllowed,
		},
		{
			desc: "post separator args not allowed",
			cmd: ForEachRefCmd{
				SubCmd: git.SubCmd{
					Name:        "for-each-ref",
					PostSepArgs: []string{"a", "b", "c"},
				},
			},
			expectedErr: ErrNoPostSeparatorArgsAllowed,
		},
		{
			desc: "valid for-each-ref command without post arg flags",
			cmd: ForEachRefCmd{
				SubCmd: git.SubCmd{
					Name:  "for-each-ref",
					Flags: []git.Option{git.Flag{"--tcl"}},
					Args:  []string{"master"},
				},
			},
			expectedArgs: []string{"for-each-ref", "--tcl", "master"},
			expectedErr:  nil,
		},
		{
			desc: "valid for-each-ref command with post arg flags",
			cmd: ForEachRefCmd{
				SubCmd: git.SubCmd{
					Name:  "for-each-ref",
					Flags: []git.Option{git.Flag{"--tcl"}},
					Args:  []string{"master"},
				},
				PostArgFlags: []git.Option{git.ValueFlag{Name: "--contains", Value: "blahblah"}},
			},
			expectedArgs: []string{"for-each-ref", "--tcl", "master", "--contains", "blahblah"},
			expectedErr:  nil,
		},
	}

	for _, tc := range testCases {
		args, err := tc.cmd.ValidateArgs()
		require.Equal(t, tc.expectedErr, err)
		require.Equal(t, tc.expectedArgs, args)
	}
}
