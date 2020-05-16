package bundle

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/safe"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	header = "# gitaly bundle v1"
)

func Create(ctx context.Context, repo *gitalypb.Repository) ([]byte, error) {
	repoPath, err := helper.GetPath(repo)
	if err != nil {
		return nil, fmt.Errorf("getting repo path: %w", err)
	}

	bundleRefsPath := filepath.Join(repoPath, ".gitaly_bundle", "refs")
	if err = os.MkdirAll(filepath.Dir(bundleRefsPath), 0755); err != nil {
		return nil, fmt.Errorf("mkdir -p %s: %w", bundleRefsPath, err)
	}

	var b bytes.Buffer

	if _, err = b.WriteString(header); err != nil {
		return nil, fmt.Errorf("writing to buffer: %w", err)
	}
	if _, err = b.Write([]byte("\n")); err != nil {
		return nil, fmt.Errorf("writing to buffer: %w", err)
	}

	bundleRefsWriter, err := safe.CreateFileWriter(bundleRefsPath)
	if err != nil {
		return nil, fmt.Errorf("creating bundle refs writer %w", err)
	}

	refWriter := io.MultiWriter(bundleRefsWriter, &b)

	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name: "for-each-ref",
		Flags: []git.Option{git.ValueFlag{
			Name:  "--format",
			Value: "%(objectname) %(refname)",
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("creating for-each-ref command: %w", err)
	}

	var objectIDs bytes.Buffer

	s := bufio.NewScanner(cmd)

	for s.Scan() {
		line := strings.SplitN(s.Text(), " ", 2)
		objectIDs.WriteString(line[0])
		objectIDs.Write([]byte("\n"))
		refWriter.Write([]byte(s.Text()))
		refWriter.Write([]byte("\n"))
	}

	if err = s.Err(); err != nil {
		return nil, fmt.Errorf("reading output of for-each-ref: %w", err)
	}

	if err = cmd.Wait(); err != nil {
		return nil, fmt.Errorf("running for-each-ref: %w", err)
	}

	if _, err = os.Stat(bundleRefsPath); err == nil {
		c, err := catfile.New(ctx, repo)
		if err != nil {
			return nil, fmt.Errorf("creating catfile batch: %w", err)
		}
		defer c.Close()

		f, err := os.Open(bundleRefsPath)

		if err != nil {
			return nil, fmt.Errorf("opening bundle refs file: %w", err)
		}
		defer f.Close()

		s = bufio.NewScanner(f)
		for s.Scan() {
			line := s.Text()
			if line == "" {
				break
			}
			if strings.HasPrefix(line, "#") {
				continue
			}
			lineSplit := strings.SplitN(line, " ", 2)

			_, err := c.Info(lineSplit[0])
			if catfile.IsNotFound(err) {
				continue
			}
			objectIDs.WriteString(fmt.Sprintf("^%s\n", lineSplit[0]))
		}

		if err = f.Close(); err != nil {
			return nil, fmt.Errorf("closing bundle refs file: %w", err)
		}
	}

	b.Write([]byte("\n"))

	cmd, err = git.SafeBareCmd(ctx, git.CmdStream{In: &objectIDs, Out: &b}, nil, []git.Option{git.ValueFlag{Name: "-C", Value: repoPath}}, git.SubCmd{
		Name:  "pack-objects",
		Flags: []git.Option{git.Flag{Name: "--thin"}, git.Flag{Name: "--stdout"}, git.Flag{Name: "--non-empty"}, git.Flag{Name: "--delta-base-offset"}},
	})
	if err != nil {
		return nil, fmt.Errorf("creating pack-objects command: %w", err)
	}

	if err = cmd.Wait(); err != nil {
		return nil, fmt.Errorf("running pack-objects command: %w", err)
	}

	if err = bundleRefsWriter.Commit(); err != nil {
		return nil, fmt.Errorf("writing new bundle refs file: %w", err)
	}

	return b.Bytes(), nil
}
