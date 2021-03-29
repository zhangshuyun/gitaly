package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/go-enry/go-license-detector/v4/licensedb/api"
	"github.com/go-enry/go-license-detector/v4/licensedb/filer"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) FindLicense(ctx context.Context, req *gitalypb.FindLicenseRequest) (*gitalypb.FindLicenseResponse, error) {
	if featureflag.IsEnabled(ctx, featureflag.GoFindLicense) {
		repo := req.GetRepository()
		if repo == nil {
			return &gitalypb.FindLicenseResponse{}, nil
		}
		repoFiler := &gitFiler{s.gitCmdFactory, ctx, repo}
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
	commander git.CommandFactory
	ctx       context.Context
	repo      repository.GitRepo
}

func (f *gitFiler) ReadFile(path string) (content []byte, err error) {
	if path == "" {
		return nil, licensedb.ErrNoLicenseFound
	}
	var stdout bytes.Buffer
	cmd, err := f.commander.New(f.ctx, f.repo, git.SubCmd{
		Name: "cat-file",
		Args: []string{"blob", fmt.Sprintf(":%s", path)},
	}, git.WithStdout(&stdout))
	if err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}
	return stdout.Bytes(), nil
}

func (f *gitFiler) ReadDir(path string) ([]filer.File, error) {
	dotPath := path
	if dotPath == "" {
		dotPath = "."
	}
	cmd, err := f.commander.New(f.ctx, f.repo, git.SubCmd{
		Name: "ls-tree",
		Flags: []git.Option{
			git.Flag{Name: "--full-tree"},
			git.Flag{Name: "-z"},
			git.Flag{Name: "-r"},
		},
		Args: []string{"HEAD", dotPath},
	})
	if err != nil {
		return nil, err
	}
	tree := lstree.NewParser(cmd)
	out := make([]filer.File, 0)
	skip := len(path)
	for {
		entry, err := tree.NextEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if entry.Type != lstree.Blob {
			continue
		}
		f := entryToFile(skip, entry)
		var duplicate bool
		for _, o := range out {
			if o.Name == f.Name {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, f)
		}
	}
	if err := cmd.Wait(); err != nil {
		exitError := &exec.ExitError{}
		if errors.As(err, &exitError) && exitError.ProcessState.ExitCode() > 0 {
			// not a git repository
			return []filer.File{}, nil
		}
		return nil, fmt.Errorf("cmd failed: %w", err)
	}
	return out, nil
}

func (f *gitFiler) Close() {}

func (f *gitFiler) PathsAreAlwaysSlash() bool {
	// git ls-files uses unix slash `/`
	return true
}

func entryToFile(skip int, entry *lstree.Entry) filer.File {
	path := entry.Path[skip:]
	idx := strings.Index(path, "/")
	var f filer.File
	if idx > 0 {
		f.Name = path[:idx]
		f.IsDir = true
	} else {
		f.Name = path
	}
	return f
}
