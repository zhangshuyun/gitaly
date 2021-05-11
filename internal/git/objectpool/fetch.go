package objectpool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	sourceRemote       = "origin"
	sourceRefNamespace = "refs/remotes/" + sourceRemote
)

// FetchFromOrigin initializes the pool and fetches the objects from its origin repository
func (o *ObjectPool) FetchFromOrigin(ctx context.Context, origin *gitalypb.Repository) error {
	if err := o.Init(ctx); err != nil {
		return err
	}

	originPath, err := o.locator.GetPath(origin)
	if err != nil {
		return err
	}

	if err := housekeeping.Perform(ctx, o.poolRepo); err != nil {
		return err
	}

	remote := o.poolRepo.Remote()

	remoteExists, err := remote.Exists(ctx, sourceRemote)
	if err != nil {
		return err
	}

	if remoteExists {
		if err := remote.SetURL(ctx, sourceRemote, originPath, git.SetURLOpts{}); err != nil {
			return err
		}
	} else {
		if err := remote.Add(ctx, sourceRemote, originPath, git.RemoteAddOpts{}); err != nil {
			return err
		}
	}

	if err := o.logStats(ctx, "before fetch"); err != nil {
		return err
	}

	refSpec := fmt.Sprintf("+refs/*:%s/*", sourceRefNamespace)
	var stderr bytes.Buffer
	if err := o.poolRepo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "fetch",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--atomic"},
			},
			Args: []string{sourceRemote, refSpec},
		},
		git.WithRefTxHook(ctx, o.poolRepo, o.cfg),
		git.WithStderr(&stderr),
	); err != nil {
		return helper.ErrInternalf("fetch into object pool: %w, stderr: %q", err,
			stderr.String())
	}

	if err := o.rescueDanglingObjects(ctx); err != nil {
		return err
	}

	if err := o.logStats(ctx, "after fetch"); err != nil {
		return err
	}

	if err := o.poolRepo.ExecAndWait(ctx, git.SubCmd{
		Name:  "pack-refs",
		Flags: []git.Option{git.Flag{Name: "--all"}},
	}); err != nil {
		return err
	}

	return o.repackPool(ctx, o)
}

const danglingObjectNamespace = "refs/dangling/"

// rescueDanglingObjects creates refs for all dangling objects if finds
// with `git fsck`, which converts those objects from "dangling" to
// "not-dangling". This guards against any object ever being deleted from
// a pool repository. This is a defense in depth against accidental use
// of `git prune`, which could remove Git objects that a pool member
// relies on. There is currently no way for us to reliably determine if
// an object is still used anywhere, so the only safe thing to do is to
// assume that every object _is_ used.
func (o *ObjectPool) rescueDanglingObjects(ctx context.Context) error {
	fsck, err := o.poolRepo.Exec(ctx, git.SubCmd{
		Name:  "fsck",
		Flags: []git.Option{git.Flag{Name: "--connectivity-only"}, git.Flag{Name: "--dangling"}},
	})
	if err != nil {
		return err
	}

	updater, err := updateref.New(ctx, o.cfg, o.poolRepo, updateref.WithDisabledTransactions())
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(fsck)
	for scanner.Scan() {
		split := strings.SplitN(scanner.Text(), " ", 3)
		if len(split) != 3 {
			continue
		}

		if split[0] != "dangling" {
			continue
		}

		ref := git.ReferenceName(danglingObjectNamespace + split[2])
		if err := updater.Create(ref, split[2]); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := fsck.Wait(); err != nil {
		return fmt.Errorf("git fsck: %v", err)
	}

	return updater.Wait()
}

func (o *ObjectPool) repackPool(ctx context.Context, pool repository.GitRepo) error {
	config := []git.ConfigPair{
		{Key: "pack.island", Value: sourceRefNamespace + "/he(a)ds"},
		{Key: "pack.island", Value: sourceRefNamespace + "/t(a)gs"},
		{Key: "pack.islandCore", Value: "a"},
		{Key: "pack.writeBitmapHashCache", Value: "true"},
	}

	if err := o.poolRepo.ExecAndWait(ctx, git.SubCmd{
		Name:  "repack",
		Flags: []git.Option{git.Flag{Name: "-aidb"}},
	}, git.WithConfig(config...)); err != nil {
		return err
	}

	return nil
}

func (o *ObjectPool) logStats(ctx context.Context, when string) error {
	fields := logrus.Fields{
		"when": when,
	}

	for key, dir := range map[string]string{
		"poolObjectsSize": "objects",
		"poolRefsSize":    "refs",
	} {
		var err error
		fields[key], err = sizeDir(ctx, filepath.Join(o.FullPath(), dir))
		if err != nil {
			return err
		}
	}

	forEachRef, err := o.poolRepo.Exec(ctx, git.SubCmd{
		Name:  "for-each-ref",
		Flags: []git.Option{git.Flag{Name: "--format=%(objecttype)%00%(refname)"}},
		Args:  []string{"refs/"},
	})
	if err != nil {
		return err
	}

	danglingRefsByType := make(map[string]int)
	normalRefsByType := make(map[string]int)

	scanner := bufio.NewScanner(forEachRef)
	for scanner.Scan() {
		line := bytes.SplitN(scanner.Bytes(), []byte{0}, 2)
		if len(line) != 2 {
			continue
		}

		objectType := string(line[0])
		refname := string(line[1])

		if strings.HasPrefix(refname, danglingObjectNamespace) {
			danglingRefsByType[objectType]++
		} else {
			normalRefsByType[objectType]++
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	if err := forEachRef.Wait(); err != nil {
		return err
	}

	for _, key := range []string{"blob", "commit", "tag", "tree"} {
		fields["dangling."+key+".ref"] = danglingRefsByType[key]
		fields["normal."+key+".ref"] = normalRefsByType[key]
	}

	ctxlogrus.Extract(ctx).WithFields(fields).Info("pool dangling ref stats")

	return nil
}

func sizeDir(ctx context.Context, dir string) (int64, error) {
	// du -k reports size in KB
	cmd, err := command.New(ctx, exec.Command("du", "-sk", dir), nil, nil, nil)
	if err != nil {
		return 0, err
	}

	sizeLine, err := ioutil.ReadAll(cmd)
	if err != nil {
		return 0, err
	}

	if err := cmd.Wait(); err != nil {
		return 0, err
	}

	sizeParts := bytes.Split(sizeLine, []byte("\t"))
	if len(sizeParts) != 2 {
		return 0, fmt.Errorf("malformed du output: %q", sizeLine)
	}

	size, err := strconv.ParseInt(string(sizeParts[0]), 10, 0)
	if err != nil {
		return 0, err
	}

	// Convert KB to B
	return size * 1024, nil
}
