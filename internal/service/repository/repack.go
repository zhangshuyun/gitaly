package repository

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const deltaIslandsFeatureFlag = "delta-islands"

var (
	repackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_repack_total",
			Help: "Counter of Git repack operations",
		},
		[]string{"bitmap", "delta_islands"},
	)
)

func init() {
	prometheus.Register(repackCounter)
}

func (server) RepackFull(ctx context.Context, in *gitalypb.RepackFullRequest) (*gitalypb.RepackFullResponse, error) {
	if err := repackCommand(ctx, in.GetRepository(), in.GetCreateBitmap(), "-A", "--pack-kept-objects", "-l"); err != nil {
		return nil, err
	}
	return &gitalypb.RepackFullResponse{}, nil
}

func (server) RepackIncremental(ctx context.Context, in *gitalypb.RepackIncrementalRequest) (*gitalypb.RepackIncrementalResponse, error) {
	if err := repackCommand(ctx, in.GetRepository(), false); err != nil {
		return nil, err
	}
	return &gitalypb.RepackIncrementalResponse{}, nil
}

func repackCommand(ctx context.Context, repo repository.GitRepo, bitmap bool, args ...string) error {
	cmdArgs := repackConfig(ctx, bitmap)

	cmdArgs = append(cmdArgs, "repack", "-d")
	cmdArgs = append(cmdArgs, args...)

	cmd, err := git.Command(ctx, repo, cmdArgs...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, err.Error())
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, err.Error())
	}

	return nil
}

func repackConfig(ctx context.Context, bitmap bool) []string {
	var args []string

	if bitmap {
		args = append(args, "-c", "repack.writeBitmaps=true")
	} else {
		args = append(args, "-c", "repack.writeBitmaps=false")
	}

	deltaIslands := featureflag.IsEnabled(ctx, deltaIslandsFeatureFlag)
	if deltaIslands {
		args = append(args,
			"-c", "pack.island=refs/heads",
			"-c", "pack.island=refs/tags",
			"-c", "repack.useDeltaIslands=true",
		)
	}

	repackCounter.WithLabelValues(fmt.Sprint(bitmap), fmt.Sprint(deltaIslands)).Inc()

	return args
}
