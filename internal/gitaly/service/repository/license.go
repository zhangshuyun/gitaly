package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/go-enry/go-license-detector/v4/licensedb/api"
	"github.com/go-enry/go-license-detector/v4/licensedb/filer"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) FindLicense(ctx context.Context, req *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	if featureflag.IsEnabled(ctx, featureflag.GoFindLicense) {
		if req.GetRepository() == nil {
			return &gitalypb.FindLicenseResponse{}, nil
		}
		repo := localrepo.New(s.gitCmdFactory, s.catfileCache, req.GetRepository(), s.cfg)

		hasHeadRevision, err := repo.HasRevision(ctx, "HEAD")
		if err != nil {
			return nil, helper.ErrInternalf("cannot check HEAD revision: %v", err)
		}
		if !hasHeadRevision {
			return &gitalypb.FindLicenseResponse{}, nil
		}

		repoFiler := &gitFiler{ctx, repo}
		defer repoFiler.Close()

		licenses, err := licensedb.Detect(repoFiler)
		if err != nil {
			if errors.Is(err, licensedb.ErrNoLicenseFound) {
				return &gitalypb.FindLicenseResponse{}, nil
			}
			return nil, helper.ErrInternal(fmt.Errorf("FindLicense: Err: %w", err))
		}

		var result string
		var best api.Match
		for candidate, match := range licenses {
			if match.Confidence > best.Confidence {
				result = candidate
			}
		}

		return &gitalypb.FindLicenseResponse{LicenseShortName: strings.ToLower(result)}, nil
	}

	client, err := s.ruby.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}
	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return nil, err
	}
	return client.FindLicense(clientCtx, req)
}

type gitFiler struct {
	ctx  context.Context
	repo *localrepo.Repo
}

func (f *gitFiler) ReadFile(path string) (content []byte, err error) {
	if path == "" {
		return nil, licensedb.ErrNoLicenseFound
	}

	var stdout, stderr bytes.Buffer
	if err := f.repo.ExecAndWait(f.ctx, git.SubCmd{
		Name: "cat-file",
		Args: []string{"blob", fmt.Sprintf(":%s", path)},
	}, git.WithStdout(&stdout), git.WithStderr(&stderr)); err != nil {
		return nil, fmt.Errorf("cat-file failed: %w, stderr: %q", err, stderr.String())
	}

	return stdout.Bytes(), nil
}

func (f *gitFiler) ReadDir(path string) ([]filer.File, error) {
	dotPath := path
	if dotPath == "" {
		dotPath = "."
	}

	// We're doing a recursive listing returning all files at once such that we do not have to
	// call git-ls-tree(1) multiple times.
	var stderr bytes.Buffer
	cmd, err := f.repo.Exec(f.ctx, git.SubCmd{
		Name: "ls-tree",
		Flags: []git.Option{
			git.Flag{Name: "--full-tree"},
			git.Flag{Name: "-z"},
			git.Flag{Name: "-r"},
		},
		Args: []string{"HEAD", dotPath},
	}, git.WithStderr(&stderr))
	if err != nil {
		return nil, err
	}

	tree := lstree.NewParser(cmd)

	var files []filer.File
	for {
		entry, err := tree.NextEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Given that we're doing a recursive listing, we skip over all types which aren't
		// blobs.
		if entry.Type != lstree.Blob {
			continue
		}

		files = append(files, filer.File{
			Name:  entry.Path,
			IsDir: false,
		})
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("ls-tree failed: %w, stderr: %q", err, stderr.String())
	}

	return files, nil
}

func (f *gitFiler) Close() {}

func (f *gitFiler) PathsAreAlwaysSlash() bool {
	// git ls-files uses unix slash `/`
	return true
}
