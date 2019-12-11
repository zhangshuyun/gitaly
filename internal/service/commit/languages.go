package commit

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (*server) CommitLanguages(ctx context.Context, req *gitalypb.CommitLanguagesRequest) (*gitalypb.CommitLanguagesResponse, error) {
	repo := req.Repository

	if err := git.ValidateRevision(req.Revision); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	revision := string(req.Revision)

	commitID, err := lookupRevision(ctx, repo, revision)
	if err != nil {
		return nil, err
	}

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	if cacheRecentlyUpdated(repoPath) {
		return nil, helper.ErrPreconditionFailedf("repository languages recently scanned")
	}

	stats, err := linguist.Stats(ctx, repoPath, commitID)
	if err != nil {
		return nil, err
	}

	// TODO: remove feature flag when Stats and FileCountStats is executed with one ruby script call
	fileCountStats := make(linguist.CountPerLanguage)
	if featureflag.IsEnabled(ctx, featureflag.LinguistFileCountStats) {
		fileCountStats, err = linguist.FileCountStats(ctx, repoPath, commitID)
		if err != nil {
			return nil, err
		}
	}

	resp := &gitalypb.CommitLanguagesResponse{}
	if len(stats) == 0 {
		return resp, nil
	}

	total := uint64(0)
	for _, count := range stats {
		total += count
	}

	if total == 0 {
		return nil, status.Errorf(codes.Internal, "linguist stats added up to zero: %v", stats)
	}

	for lang, count := range stats {
		l := &gitalypb.CommitLanguagesResponse_Language{
			Name:      lang,
			Share:     float32(100*count) / float32(total),
			Color:     linguist.Color(lang),
			FileCount: uint32(fileCountStats[lang]),
			Bytes:     uint64(stats[lang]),
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
	revParse, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{Name: "rev-parse", Args: []string{revision}})
	if err != nil {
		return "", err
	}

	revParseBytes, err := ioutil.ReadAll(revParse)
	if err != nil {
		return "", err
	}

	return text.ChompBytes(revParseBytes), nil
}

// Gitaly uses Linguist to detect the languages, and to improve performance, the
// results will be cached. If the modified time is later than a trashold it's
// considered recently updated enough that we do not scan again
func cacheRecentlyUpdated(path string) bool {
	fullPath := filepath.Join(path, "language-stats.cache")

	fi, err := os.Stat(fullPath)
	if err != nil {
		return false
	}

	const sentinal = -time.Hour * 24 * 7
	return fi.ModTime().After(time.Now().Add(sentinal))
}
