package housekeeping

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
)

// RepackObjectsConfig is configuration for RepackObjects.
type RepackObjectsConfig struct {
	// FullRepack determines whether all reachable objects should be repacked into a single
	// packfile. This is much less efficient than doing incremental repacks, which only soak up
	// all loose objects into a new packfile.
	FullRepack bool
	// WriteBitmap determines whether reachability bitmaps should be written or not. There is no
	// reason to set this to `false`, except for legacy compatibility reasons with existing RPC
	// behaviour
	WriteBitmap bool
}

// RepackObjects repacks objects in the given repository and updates the commit-graph. The way
// objects are repacked is determined via the RepackObjectsConfig.
func RepackObjects(ctx context.Context, repo *localrepo.Repo, cfg RepackObjectsConfig) error {
	var options []git.Option
	if cfg.FullRepack {
		options = append(options,
			git.Flag{Name: "-A"},
			git.Flag{Name: "--pack-kept-objects"},
			git.Flag{Name: "-l"},
			log2Threads(runtime.NumCPU()),
		)
	}

	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name:  "repack",
		Flags: append([]git.Option{git.Flag{Name: "-d"}}, options...),
	}, git.WithConfig(GetRepackGitConfig(ctx, cfg.WriteBitmap)...)); err != nil {
		return err
	}

	if err := WriteCommitGraph(ctx, repo); err != nil {
		return err
	}

	stats.LogObjectsInfo(ctx, repo)

	return nil
}

// GetRepackGitConfig returns configuration suitable for Git commands which write new packfiles.
func GetRepackGitConfig(ctx context.Context, bitmap bool) []git.ConfigPair {
	config := []git.ConfigPair{
		{Key: "pack.island", Value: "r(e)fs/heads"},
		{Key: "pack.island", Value: "r(e)fs/tags"},
		{Key: "pack.islandCore", Value: "e"},
		{Key: "repack.useDeltaIslands", Value: "true"},
	}

	if bitmap {
		config = append(config, git.ConfigPair{Key: "repack.writeBitmaps", Value: "true"})
		config = append(config, git.ConfigPair{Key: "pack.writeBitmapHashCache", Value: "true"})
	} else {
		config = append(config, git.ConfigPair{Key: "repack.writeBitmaps", Value: "false"})
	}

	return config
}

// log2Threads returns the log-2 number of threads based on the number of
// provided CPUs. This prevents repacking operations from exhausting all
// available CPUs and increasing request latency
func log2Threads(numCPUs int) git.ValueFlag {
	n := math.Max(1, math.Floor(math.Log2(float64(numCPUs))))
	return git.ValueFlag{Name: "--threads", Value: fmt.Sprint(n)}
}
