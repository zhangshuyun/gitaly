package commit

import (
	"context"
	"io/ioutil"
	"sort"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (*server) CommitLanguages(ctx context.Context, req *gitalypb.CommitLanguagesRequest) (*gitalypb.CommitLanguagesResponse, error) {
	repo := req.Repository

	if err := git.ValidateRevisionAllowEmpty(req.Revision); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	revision := string(req.Revision)
	if revision == "" {
		defaultBranch, err := ref.DefaultBranchName(ctx, req.Repository)
		if err != nil {
			return nil, err
		}
		revision = string(defaultBranch)
	}

	commitID, err := lookupRevision(ctx, repo, revision)
	if err != nil {
		return nil, err
	}

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}
	stats, err := linguist.Stats(ctx, repoPath, commitID)
	if err != nil {
		return nil, err
	}

	resp := &gitalypb.CommitLanguagesResponse{}
	if len(stats) == 0 {
		return resp, nil
	}

	total := 0
	for _, count := range stats {
		total += count
	}

	if total == 0 {
		return nil, status.Errorf(codes.Internal, "linguist stats added up to zero: %v", stats)
	}

	for lang, count := range stats {
		l := &gitalypb.CommitLanguagesResponse_Language{
			Name:  lang,
			Share: float32(100*count) / float32(total),
			Color: linguist.Color(lang),
		}
		resp.Languages = append(resp.Languages, l)
	}

	sort.Sort(languageSorter(resp.Languages))

	return resp, nil
}

type languageSorter []*gitalypb.CommitLanguagesResponse_Language

func (ls languageSorter) Len() int           { return len(ls) }
func (ls languageSorter) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls languageSorter) Less(i, j int) bool { return ls[i].Share > ls[j].Share }

func lookupRevision(ctx context.Context, repo *gitalypb.Repository, revision string) (string, error) {
	revParse, err := git.Command(ctx, repo, "rev-parse", revision)
	if err != nil {
		return "", err
	}

	revParseBytes, err := ioutil.ReadAll(revParse)
	if err != nil {
		return "", err
	}

	return text.ChompBytes(revParseBytes), nil
}
