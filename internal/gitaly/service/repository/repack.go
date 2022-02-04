package repository

import (
	"context"
	"fmt"
	"math"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

var repackCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_repack_total",
		Help: "Counter of Git repack operations",
	},
	[]string{"bitmap"},
)

func init() {
	prometheus.MustRegister(repackCounter)
}

// log2Threads returns the log-2 number of threads based on the number of
// provided CPUs. This prevents repacking operations from exhausting all
// available CPUs and increasing request latency
func log2Threads(numCPUs int) git.ValueFlag {
	n := math.Max(1, math.Floor(math.Log2(float64(numCPUs))))
	return git.ValueFlag{Name: "--threads", Value: fmt.Sprint(n)}
}

func (s *server) RepackFull(ctx context.Context, in *gitalypb.RepackFullRequest) (*gitalypb.RepackFullResponse, error) {
	repo := s.localrepo(in.GetRepository())
	options := []git.Option{
		git.Flag{Name: "-A"},
		git.Flag{Name: "--pack-kept-objects"},
		git.Flag{Name: "-l"},
		log2Threads(runtime.NumCPU()),
	}
	if err := repack(ctx, repo, in.GetCreateBitmap(), options...); err != nil {
		return nil, helper.ErrInternal(err)
	}
	return &gitalypb.RepackFullResponse{}, nil
}

func (s *server) RepackIncremental(ctx context.Context, in *gitalypb.RepackIncrementalRequest) (*gitalypb.RepackIncrementalResponse, error) {
	repo := s.localrepo(in.GetRepository())
	if err := repack(ctx, repo, false); err != nil {
		return nil, helper.ErrInternal(err)
	}
	return &gitalypb.RepackIncrementalResponse{}, nil
}

func repack(ctx context.Context, repo *localrepo.Repo, bitmap bool, args ...git.Option) error {
	if err := repo.ExecAndWait(ctx, git.SubCmd{
		Name:  "repack",
		Flags: append([]git.Option{git.Flag{Name: "-d"}}, args...),
	}, git.WithConfig(repackConfig(ctx, bitmap)...)); err != nil {
		return err
	}

	if err := writeCommitGraph(ctx, repo, gitalypb.WriteCommitGraphRequest_SizeMultiple); err != nil {
		return err
	}

	stats.LogObjectsInfo(ctx, repo)

	return nil
}

func repackConfig(ctx context.Context, bitmap bool) []git.ConfigPair {
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

	repackCounter.WithLabelValues(fmt.Sprint(bitmap)).Inc()

	return config
}
