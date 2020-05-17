package bundle

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func Unpack(ctx context.Context, repo *gitalypb.Repository, bundle io.Reader) error {
	r := bufio.NewReader(bundle)

	line, err := readLine(r)
	if err != nil {
		return err
	}

	const header = "# gitaly bundle v1"
	if line != header {
		return fmt.Errorf("invalid header %q", line)
	}

	// Ref dump is at the head of the bundle so we must consume it first
	newRefs, err := refMapFromReader(r)
	if err != nil {
		return err
	}

	// There may not be a pack file, for instance if the only change is a ref deletion.
	const magic = "PACK"
	if b, err := r.Peek(len(magic)); err == nil && string(b) == magic {
		// TODO use --keep to prevent concurrent repack/gc from deleting this
		// packfile before we apply the ref update below?

		cmd, err := git.SafeStdinCmd(ctx, repo, nil, git.SubCmd{
			Name: "index-pack",
			Flags: []git.Option{
				git.Flag{
					Name: "--stdin",
				},
				git.Flag{
					Name: "--fix-thin",
				},
				git.Flag{
					Name: "--keep",
				},
			},
			Args:        nil,
			PostSepArgs: nil,
		})
		if err != nil {
			return err
		}

		if _, err = io.Copy(cmd, r); err != nil {
			return err
		}

		if err = cmd.Wait(); err != nil {
			return err
		}
	}

	updater, err := updateref.New(ctx, repo)
	if err != nil {
		return err
	}

	oldRefs, err := currentRefs(ctx, repo)
	if err != nil {
		return err
	}

	for ref, oid := range newRefs {
		if err = updater.Update(ref, oid, "0000000000000000000000000000000000000000"); err != nil {
			return err
		}
	}

	for ref := range oldRefs {
		if _, ok := newRefs[ref]; ok {
			continue
		}
		if err = updater.Delete(ref); err != nil {
			return err
		}
	}

	if err = updater.Wait(); err != nil {
		return err
	}

	return nil
}

func refMapFromReader(r *bufio.Reader) (map[string]string, error) {
	result := make(map[string]string)
	var err error

	for {
		line, err := readLine(r)
		if err != nil || line == "" {
			break
		}

		split := strings.SplitN(line, " ", 2)
		if len(split) != 2 {
			return nil, fmt.Errorf("invalid ref line %q", line)
		}
		result[split[1]] = split[0]
	}

	return result, err
}

func readLine(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	return string(b)[:len(b)-1], err
}

func currentRefs(ctx context.Context, repo *gitalypb.Repository) (map[string]string, error) {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name: "for-each-ref",
		Flags: []git.Option{
			git.ValueFlag{
				Name:  "--format",
				Value: "%(objectname) %(refname)",
			},
		},
	})

	if err != nil {
		return nil, err
	}

	refs, err := refMapFromReader(bufio.NewReader(cmd))
	if err == io.EOF {
		err = nil
	}

	return refs, err
}
