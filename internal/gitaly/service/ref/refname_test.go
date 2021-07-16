package ref

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRefNameSuccess(t *testing.T) {
	_, repo, _, client := setupRefService(t)

	rpcRequest := &gitalypb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	c, err := client.FindRefName(ctx, rpcRequest)
	require.NoError(t, err)

	response := string(c.GetName())

	if response != `refs/heads/expand-collapse-diffs` {
		t.Errorf("Expected FindRefName to return `refs/heads/expand-collapse-diffs`, got `%#v`", response)
	}
}

func TestFindRefNameEmptyCommit(t *testing.T) {
	_, repo, _, client := setupRefService(t)

	rpcRequest := &gitalypb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := testhelper.Context()
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
	_, client := setupRefServiceWithoutRepo(t)

	repo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}
	rpcRequest := &gitalypb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/heads/`),
	}

	ctx, cancel := testhelper.Context()
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
	_, repo, _, client := setupRefService(t)

	rpcRequest := &gitalypb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
		Prefix:     []byte(`refs/nonexistant/`),
	}

	ctx, cancel := testhelper.Context()
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
	_, repo, _, client := setupRefService(t)

	rpcRequest := &gitalypb.FindRefNameRequest{
		Repository: repo,
		CommitId:   "dead1234dead1234dead1234dead1234dead1234",
	}

	ctx, cancel := testhelper.Context()
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
		cmd          git.Cmd
		expectedErr  error
		expectedArgs []string
	}{
		{
			desc: "post separator args allowed",
			cmd: git.SubCmd{
				Name:        "for-each-ref",
				PostSepArgs: []string{"a", "b", "c"},
			},
			expectedArgs: []string{
				"for-each-ref", "--end-of-options", "--", "a", "b", "c",
			},
		},
		{
			desc: "valid for-each-ref command without post arg flags",
			cmd: git.SubCmd{
				Name:  "for-each-ref",
				Flags: []git.Option{git.Flag{Name: "--tcl"}},
				Args:  []string{"master"},
			},
			expectedArgs: []string{
				"for-each-ref", "--tcl", "--end-of-options", "master",
			},
		},
		{
			desc: "invalid for-each-ref command with post arg flags",
			cmd: git.SubCmd{
				Name:  "for-each-ref",
				Flags: []git.Option{git.Flag{Name: "--tcl"}},
				Args:  []string{"master", "--contains=blahblah"},
			},
			expectedErr: fmt.Errorf("positional arg %q cannot start with dash '-': %w", "--contains=blahblah", git.ErrInvalidArg),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			args, err := tc.cmd.CommandArgs()
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedArgs, args)
		})
	}
}
