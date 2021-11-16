package backup

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestShowRefDecoder(t *testing.T) {
	cfg := testcfg.Build(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoProto, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: "repo.git",
	})

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	expectedRefs, err := repo.GetReferences(ctx, "refs/")
	require.NoError(t, err)

	output := gittest.Exec(t, cfg, "-C", repoPath, "show-ref")
	stream := bytes.NewBuffer(output)

	d := NewShowRefDecoder(stream)

	var refs []git.Reference
	for {
		var ref git.Reference

		err := d.Decode(&ref)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		refs = append(refs, ref)
	}

	require.Equal(t, expectedRefs, refs)
}
