package commit

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAmbigRef = errors.New("ambiguous reference")

func (s *server) CommitLanguages(ctx context.Context, req *gitalypb.CommitLanguagesRequest) (*gitalypb.CommitLanguagesResponse, error) {
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

	repoPath, err := s.locator.GetRepoPath(repo)
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

	total := uint64(0)
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
			Bytes: stats[lang],
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
	repoPath, env, err := alternates.PathAndEnv(repo)
	if err != nil {
		return "", err
	}

	rev, err := checkRevision(ctx, repoPath, env, revision)
	if err != nil {
		switch err {
		case errAmbigRef:
			fullRev, err := disambiguateRevision(ctx, repo, revision)
			if err != nil {
				return "", err
			}

			rev, err = checkRevision(ctx, repoPath, env, fullRev)
			if err != nil {
				return "", err
			}
		default:
			return "", err
		}
	}

	return rev, nil
}

func checkRevision(ctx context.Context, repoPath string, env []string, revision string) (string, error) {
	opts := []git.Option{git.ValueFlag{"-C", repoPath}}
	var stdout, stderr bytes.Buffer

	revParse, err := git.SafeBareCmd(ctx, git.CmdStream{Out: &stdout, Err: &stderr}, env, opts,
		git.SubCmd{Name: "rev-parse", Args: []string{revision}},
	)

	if err != nil {
		return "", err
	}

	if err = revParse.Wait(); err != nil {
		errMsg := strings.Split(stderr.String(), "\n")[0]
		return "", fmt.Errorf("%v: %v", err, errMsg)
	}

	if strings.HasSuffix(stderr.String(), "refname '"+revision+"' is ambiguous.\n") {
		return "", errAmbigRef
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

func disambiguateRevision(ctx context.Context, repo *gitalypb.Repository, revision string) (string, error) {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name:  "for-each-ref",
		Flags: []git.Option{git.Flag{Name: "--format=%(refname)"}},
		Args:  []string{"**/" + revision},
	})

	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		refName := scanner.Text()

		if strings.HasPrefix(refName, "refs/heads") {
			return refName, nil
		}
	}

	return "", fmt.Errorf("branch %v not found", revision)
}
