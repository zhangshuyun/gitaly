package housekeeping

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

// OptimizeRepository performs optimizations on the repository. Whether optimizations are performed
// or not depends on a set of heuristics.
func (m *RepositoryManager) OptimizeRepository(ctx context.Context, repo *localrepo.Repo) error {
	totalTimer := prometheus.NewTimer(m.tasksLatency.WithLabelValues("total"))

	optimizations := struct {
		PackedObjects bool `json:"packed_objects"`
		PrunedObjects bool `json:"pruned_objects"`
		PackedRefs    bool `json:"packed_refs"`
	}{}
	defer func() {
		totalTimer.ObserveDuration()
		ctxlogrus.Extract(ctx).WithField("optimizations", optimizations).Info("optimized repository")

		for task, executed := range map[string]bool{
			"packed_objects": optimizations.PackedObjects,
			"pruned_objects": optimizations.PrunedObjects,
			"packed_refs":    optimizations.PackedRefs,
		} {
			if executed {
				m.tasksTotal.WithLabelValues(task).Add(1)
			}
		}
	}()

	timer := prometheus.NewTimer(m.tasksLatency.WithLabelValues("clean-stale-data"))
	if err := m.CleanStaleData(ctx, repo); err != nil {
		return fmt.Errorf("could not execute houskeeping: %w", err)
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("clean-worktrees"))
	if err := CleanupWorktrees(ctx, repo); err != nil {
		return fmt.Errorf("could not clean up worktrees: %w", err)
	}
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("repack"))
	didRepack, err := repackIfNeeded(ctx, repo)
	if err != nil {
		return fmt.Errorf("could not repack: %w", err)
	}
	optimizations.PackedObjects = didRepack
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("prune"))
	didPrune, err := pruneIfNeeded(ctx, repo)
	if err != nil {
		return fmt.Errorf("could not prune: %w", err)
	}
	optimizations.PrunedObjects = didPrune
	timer.ObserveDuration()

	timer = prometheus.NewTimer(m.tasksLatency.WithLabelValues("pack-refs"))
	didPackRefs, err := packRefsIfNeeded(ctx, repo)
	if err != nil {
		return fmt.Errorf("could not pack refs: %w", err)
	}
	optimizations.PackedRefs = didPackRefs
	timer.ObserveDuration()

	return nil
}

// repackIfNeeded uses a set of heuristics to determine whether the repository needs a
// full repack and, if so, repacks it.
func repackIfNeeded(ctx context.Context, repo *localrepo.Repo) (bool, error) {
	repackNeeded, cfg, err := needsRepacking(repo)
	if err != nil {
		return false, fmt.Errorf("determining whether repo needs repack: %w", err)
	}

	if !repackNeeded {
		return false, nil
	}

	if err := RepackObjects(ctx, repo, cfg); err != nil {
		return false, err
	}

	return true, nil
}

func needsRepacking(repo *localrepo.Repo) (bool, RepackObjectsConfig, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return false, RepackObjectsConfig{}, fmt.Errorf("getting repository path: %w", err)
	}

	altFile, err := repo.InfoAlternatesPath()
	if err != nil {
		return false, RepackObjectsConfig{}, helper.ErrInternal(err)
	}

	hasAlternate := true
	if _, err := os.Stat(altFile); os.IsNotExist(err) {
		hasAlternate = false
	}

	hasBitmap, err := stats.HasBitmap(repoPath)
	if err != nil {
		return false, RepackObjectsConfig{}, fmt.Errorf("checking for bitmap: %w", err)
	}

	// Bitmaps are used to efficiently determine transitive reachability of objects from a
	// set of commits. They are an essential part of the puzzle required to serve fetches
	// efficiently, as we'd otherwise need to traverse the object graph every time to find
	// which objects we have to send. We thus repack the repository with bitmaps enabled in
	// case they're missing.
	//
	// There is one exception: repositories which are connected to an object pool must not have
	// a bitmap on their own. We do not yet use multi-pack indices, and in that case Git can
	// only use one bitmap. We already generate this bitmap in the pool, so member of it
	// shouldn't have another bitmap on their own.
	if !hasBitmap && !hasAlternate {
		return true, RepackObjectsConfig{
			FullRepack:  true,
			WriteBitmap: true,
		}, nil
	}

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	if err != nil {
		return false, RepackObjectsConfig{}, fmt.Errorf("checking for bloom filters: %w", err)
	}

	// Bloom filters are part of the commit-graph and allow us to efficiently determine which
	// paths have been modified in a given commit without having to look into the object
	// database. In the past we didn't compute bloom filters at all, so we want to rewrite the
	// whole commit-graph to generate them.
	//
	// Note that we'll eventually want to move out commit-graph generation from repacking. When
	// that happens we should update the commit-graph either if it's missing, when bloom filters
	// are missing or when packfiles have been updated.
	if missingBloomFilters {
		return true, RepackObjectsConfig{
			FullRepack:  true,
			WriteBitmap: !hasAlternate,
		}, nil
	}

	largestPackfileSize, packfileCount, err := packfileSizeAndCount(repo)
	if err != nil {
		return false, RepackObjectsConfig{}, fmt.Errorf("checking largest packfile size: %w", err)
	}

	// Whenever we do an incremental repack we create a new packfile, and as a result Git may
	// have to look into every one of the packfiles to find objects. This is less efficient the
	// more packfiles we have, but we cannot repack the whole repository every time either given
	// that this may take a lot of time.
	//
	// Instead, we determine whether the repository has "too many" packfiles. "Too many" is
	// relative though: for small repositories it's fine to do full repacks regularly, but for
	// large repositories we need to be more careful. We thus use a heuristic of "repository
	// largeness": we take the biggest packfile that exists, and then the maximum allowed number
	// of packfiles is `log(largestpackfile_size_in_mb) / log(1.3)`. This gives the following
	// allowed number of packfiles:
	//
	// - No packfile: 5 packfile. This is a special case.
	// - 10MB packfile: 8 packfiles.
	// - 100MB packfile: 17 packfiles.
	// - 500MB packfile: 23 packfiles.
	// - 1GB packfile: 26 packfiles.
	// - 5GB packfile: 32 packfiles.
	// - 10GB packfile: 35 packfiles.
	// - 100GB packfile: 43 packfiles.
	//
	// The goal is to have a comparatively quick ramp-up of allowed packfiles as the repository
	// size grows, but then slow down such that we're effectively capped and don't end up with
	// an excessive amount of packfiles.
	//
	// This is a heuristic and thus imperfect by necessity. We may tune it as we gain experience
	// with the way it behaves.
	if int64(math.Max(5, math.Log(float64(largestPackfileSize))/math.Log(1.3))) < packfileCount {
		return true, RepackObjectsConfig{
			FullRepack:  true,
			WriteBitmap: !hasAlternate,
		}, nil
	}

	looseObjectCount, err := estimateLooseObjectCount(repo)
	if err != nil {
		return false, RepackObjectsConfig{}, fmt.Errorf("estimating loose object count: %w", err)
	}

	// Most Git commands do not write packfiles directly, but instead write loose objects into
	// the object database. So while we now know that there ain't too many packfiles, we still
	// need to check whether we have too many objects.
	//
	// In this case it doesn't make a lot of sense to scale incremental repacks with the repo's
	// size: we only pack loose objects, so the time to pack them doesn't scale with repository
	// size but with the number of loose objects we have. git-gc(1) uses a threshold of 6700
	// loose objects to start an incremental repack, but one needs to keep in mind that Git
	// typically has defaults which are better suited for the client-side instead of the
	// server-side in most commands.
	//
	// In our case we typically want to ensure that our repositories are much better packed than
	// it is necessary on the client side. We thus take a much stricter limit of 1024 objects.
	if looseObjectCount > looseObjectLimit {
		return true, RepackObjectsConfig{
			FullRepack:  false,
			WriteBitmap: false,
		}, nil
	}

	return false, RepackObjectsConfig{}, nil
}

func packfileSizeAndCount(repo *localrepo.Repo) (int64, int64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, 0, fmt.Errorf("getting repository path: %w", err)
	}

	entries, err := os.ReadDir(filepath.Join(repoPath, "objects/pack"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil
		}

		return 0, 0, err
	}

	largestSize := int64(0)
	count := int64(0)

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}

		entryInfo, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return 0, 0, fmt.Errorf("getting packfile info: %w", err)
		}

		if entryInfo.Size() > largestSize {
			largestSize = entryInfo.Size()
		}

		count++
	}

	return largestSize / 1024 / 1024, count, nil
}

// estimateLooseObjectCount estimates the number of loose objects in the repository. Due to the
// object name being derived via a cryptographic hash we know that in the general case, objects are
// evenly distributed across their sharding directories. We can thus estimate the number of loose
// objects by opening a single sharding directory and counting its entries.
func estimateLooseObjectCount(repo *localrepo.Repo) (int64, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return 0, fmt.Errorf("getting repository path: %w", err)
	}

	// We use the same sharding directory as git-gc(1) does for its estimation.
	entries, err := os.ReadDir(filepath.Join(repoPath, "objects/17"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("reading loose object shard: %w", err)
	}

	looseObjects := int64(0)
	for _, entry := range entries {
		if strings.LastIndexAny(entry.Name(), "0123456789abcdef") != len(entry.Name())-1 {
			continue
		}

		looseObjects++
	}

	// Scale up found loose objects by the number of sharding directories.
	return looseObjects * 256, nil
}

// pruneIfNeeded removes objects from the repository which are either unreachable or which are
// already part of a packfile. We use a grace period of two weeks.
func pruneIfNeeded(ctx context.Context, repo *localrepo.Repo) (bool, error) {
	// Pool repositories must never prune any objects, or otherwise we may corrupt members of
	// that pool if they still refer to that object.
	if strings.HasPrefix(repo.GetRelativePath(), "@pools") {
		return false, nil
	}

	looseObjectCount, err := estimateLooseObjectCount(repo)
	if err != nil {
		return false, fmt.Errorf("estimating loose object count: %w", err)
	}

	// We again use the same limit here as we do when doing an incremental repack. This is done
	// intentionally: if we determine that there's too many loose objects and try to repack, but
	// all of those loose objects are in fact unreachable, then we'd still have the same number
	// of unreachable objects after the incremental repack. We'd thus try to repack every single
	// time.
	//
	// Using the same limit here doesn't quite fix this case: the unreachable objects would only
	// be pruned after a grace period of two weeks. But at least we know that we will eventually
	// prune up those unreachable objects, at which point we won't try to do another incremental
	// repack.
	if looseObjectCount <= looseObjectLimit {
		return false, nil
	}

	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "prune",
	}); err != nil {
		return false, fmt.Errorf("pruning objects: %w", err)
	}

	return true, nil
}

func packRefsIfNeeded(ctx context.Context, repo *localrepo.Repo) (bool, error) {
	repoPath, err := repo.Path()
	if err != nil {
		return false, fmt.Errorf("getting repository path: %w", err)
	}
	refsPath := filepath.Join(repoPath, "refs")

	looseRefs := int64(0)
	if err := filepath.WalkDir(refsPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() {
			looseRefs++
		}

		return nil
	}); err != nil {
		return false, fmt.Errorf("counting loose refs: %w", err)
	}

	// If there aren't any loose refs then there is nothing we need to do.
	if looseRefs == 0 {
		return false, nil
	}

	packedRefsSize := int64(0)
	if stat, err := os.Stat(filepath.Join(repoPath, "packed-refs")); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("getting packed-refs size: %w", err)
		}
	} else {
		packedRefsSize = stat.Size()
	}

	// Packing loose references into the packed-refs file scales with the number of references
	// we're about to write. We thus decide whether we repack refs by weighing the current size
	// of the packed-refs file against the number of loose references. This is done such that we
	// do not repack too often on repositories with a huge number of references, where we can
	// expect a lot of churn in the number of references.
	//
	// As a heuristic, we repack if the number of loose references in the repository exceeds
	// `log(packed_refs_size_in_bytes/100)/log(1.15)`, which scales as following (number of refs
	// is estimated with 100 bytes per reference):
	//
	// - 1kB ~ 10 packed refs: 16 refs
	// - 10kB ~ 100 packed refs: 33 refs
	// - 100kB ~ 1k packed refs: 49 refs
	// - 1MB ~ 10k packed refs: 66 refs
	// - 10MB ~ 100k packed refs: 82 refs
	// - 100MB ~ 1m packed refs: 99 refs
	//
	// We thus allow roughly 16 additional loose refs per factor of ten of packed refs.
	//
	// This heuristic may likely need tweaking in the future, but should serve as a good first
	// iteration.
	if int64(math.Max(16, math.Log(float64(packedRefsSize)/100)/math.Log(1.15))) > looseRefs {
		return false, nil
	}

	var stderr bytes.Buffer
	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name: "pack-refs",
		Flags: []git.Option{
			git.Flag{Name: "--all"},
		},
	}, git.WithStderr(&stderr)); err != nil {
		return false, fmt.Errorf("packing refs: %w, stderr: %q", err, stderr.String())
	}

	return true, nil
}
