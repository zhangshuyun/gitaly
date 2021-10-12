package ref

import (
	"bufio"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func (s *server) ForEachRefWithObjects(req *gitalypb.ForEachRefWithObjectsRequest, stream gitalypb.RefService_ForEachRefWithObjectsServer) error {
	ctx := stream.Context()

	var patterns []string
	for _, p := range req.GetPatterns() {
		patterns = append(patterns, string(p))
	}
	forEachRefFlags := []git.Option{
		git.ValueFlag{
			Name:  "--format",
			Value: "%(objectname) %(refname)%(if:equals=tag)%(objecttype)%(then)%0a%(objectname)^{} %(refname)^{}%(end)",
		},
	}
	if sort := req.GetSort(); sort != "" {
		forEachRefFlags = append(forEachRefFlags, git.ValueFlag{Name: "--sort", Value: sort})
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		return err
	}
	defer pr.Close()
	defer pw.Close()

	repo := s.localrepo(req.GetRepository())
	forEachRef, err := repo.Exec(ctx,
		git.SubCmd{
			Name:  "for-each-ref",
			Flags: forEachRefFlags,
			Args:  patterns,
		},
		git.WithStdout(pw),
	)
	if err != nil {
		return err
	}
	if err := pw.Close(); err != nil {
		return err
	}

	stdout := bufio.NewWriterSize(streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.ForEachRefWithObjectsResponse{Data: p})
	}), 32*1024)

	catFile, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--buffer"},
				git.Flag{Name: "--batch=%(objectname) %(objecttype) %(objectsize) %(rest)"},
			},
		},
		git.WithStdin(pr),
		git.WithStdout(stdout),
	)
	if err != nil {
		return err
	}
	if err := pr.Close(); err != nil {
		return err
	}

	if err := catFile.Wait(); err != nil {
		return err
	}
	if err := forEachRef.Wait(); err != nil {
		return err
	}
	if err := stdout.Flush(); err != nil {
		return err
	}

	return nil
}
