package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/go-enry/go-license-detector/v4/licensedb/filer"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) FindLicense(ctx context.Context, req *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	if featureflag.GoFindLicense.IsEnabled(ctx) {
		repo := localrepo.New(s.gitCmdFactory, s.catfileCache, req.GetRepository(), s.cfg)

		hasHeadRevision, err := repo.HasRevision(ctx, "HEAD")
		if err != nil {
			return nil, helper.ErrInternalf("cannot check HEAD revision: %v", err)
		}
		if !hasHeadRevision {
			return &gitalypb.FindLicenseResponse{}, nil
		}

		repoFiler := &gitFiler{ctx, repo, false}

		licenses, err := licensedb.Detect(repoFiler)
		if err != nil {
			if errors.Is(err, licensedb.ErrNoLicenseFound) {
				licenseShortName := ""
				if repoFiler.foundLicense {
					// The Ruby implementation of FindLicense returned 'other' when a license file
					// was found and '' when no license file was found. `Detect` method returns ErrNoLicenseFound
					// if it doesn't identify the license. To retain backwards compatibility, the repoFiler records
					// whether it encountered any license files. That information is used here to then determine that
					// we need to send back 'other'.
					licenseShortName = "other"
				}

				return &gitalypb.FindLicenseResponse{LicenseShortName: licenseShortName}, nil
			}
			return nil, helper.ErrInternal(fmt.Errorf("FindLicense: Err: %w", err))
		}

		var result string
		var bestConfidence float32
		for candidate, match := range licenses {
			if match.Confidence > bestConfidence {
				result = candidate
				bestConfidence = match.Confidence
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

var readmeRegexp = regexp.MustCompile(`(readme|guidelines)(\.md|\.rst|\.html|\.txt)?$`)

type gitFiler struct {
	ctx          context.Context
	repo         *localrepo.Repo
	foundLicense bool
}

func (f *gitFiler) ReadFile(path string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	if err := f.repo.ExecAndWait(f.ctx, git.SubCmd{
		Name: "cat-file",
		Args: []string{"blob", fmt.Sprintf("HEAD:%s", path)},
	}, git.WithStdout(&stdout), git.WithStderr(&stderr)); err != nil {
		return nil, fmt.Errorf("cat-file failed: %w, stderr: %q", err, stderr.String())
	}

	// `licensedb.Detect` only opens files that look like licenses. Failing that, it will
	// also open readme files to try to identify license files. The RPC handler needs the
	// knowledge of whether any license files were encountered, so we filter out the
	// readme files as defined in licensedb.Detect:
	// https://github.com/go-enry/go-license-detector/blob/4f2ca6af2ab943d9b5fa3a02782eebc06f79a5f4/licensedb/internal/investigation.go#L61
	//
	// This doesn't filter out the possible license files identified from the readme files which may infact not
	// be licenses.
	if !f.foundLicense {
		f.foundLicense = !readmeRegexp.MatchString(strings.ToLower(path))
	}

	return stdout.Bytes(), nil
}

func (f *gitFiler) ReadDir(string) ([]filer.File, error) {
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
		Args: []string{"HEAD"},
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
