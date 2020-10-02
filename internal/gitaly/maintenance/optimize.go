package maintenance

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var repoOptimizationHistogram = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "gitaly_daily_maintenance_repo_optimization_seconds",
		Help:    "How many seconds each repo takes to successfully optimize during daily maintenance",
		Buckets: []float64{0.01, 0.1, 1.0, 10.0, 100},
	},
)

func init() {
	prometheus.MustRegister(repoOptimizationHistogram)
}

func shuffledStoragesCopy(randSrc *rand.Rand, storages []config.Storage) []config.Storage {
	shuffled := make([]config.Storage, len(storages))
	copy(shuffled, storages)
	randSrc.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return shuffled
}

func shuffleFileInfos(randSrc *rand.Rand, s []os.FileInfo) {
	randSrc.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
}

// Optimizer knows how to optimize a repository
type Optimizer interface {
	OptimizeRepository(context.Context, *gitalypb.OptimizeRepositoryRequest, ...grpc.CallOption) (*gitalypb.OptimizeRepositoryResponse, error)
}

func optimizeRepoAtPath(ctx context.Context, l logrus.FieldLogger, s config.Storage, absPath string, o Optimizer) error {
	relPath, err := filepath.Rel(s.Path, absPath)
	if err != nil {
		return err
	}

	repo := &gitalypb.Repository{
		StorageName:  s.Name,
		RelativePath: relPath,
	}

	optimizeReq := &gitalypb.OptimizeRepositoryRequest{
		Repository: repo,
	}

	start := time.Now()
	if _, err := o.OptimizeRepository(ctx, optimizeReq); err != nil {
		l.WithFields(map[string]interface{}{
			"relative_path": relPath,
			"storage":       s.Name,
		}).WithError(err).
			Errorf("maintenance: repo optimization failure")
	}
	repoOptimizationHistogram.Observe(time.Since(start).Seconds())

	return nil
}

func walkReposShuffled(ctx context.Context, randSrc *rand.Rand, l logrus.FieldLogger, path string, s config.Storage, o Optimizer) error {
	entries, err := ioutil.ReadDir(path)
	switch {
	case os.IsNotExist(err):
		return nil // race condition: someone deleted it
	case err != nil:
		return err
	}

	shuffleFileInfos(randSrc, entries)

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		if !e.IsDir() {
			continue
		}

		absPath := filepath.Join(path, e.Name())
		if !storage.IsGitDirectory(absPath) {
			if err := walkReposShuffled(ctx, randSrc, l, absPath, s, o); err != nil {
				return err
			}
			continue
		}

		if err := optimizeRepoAtPath(ctx, l, s, absPath, o); err != nil {
			return err
		}
	}

	return nil
}

// OptimizeReposRandomly returns a function to walk through each storage and
// attempt to optimize any repos encountered.
//
// Only storage paths that map to an enabled storage name will be walked.
// Any storage paths shared by multiple storages will only be walked once.
//
// Any errors during the optimization will be logged. Any other errors will be
// returned and cause the walk to end prematurely.
func OptimizeReposRandomly(storages []config.Storage, optimizer Optimizer) StoragesJob {
	return func(ctx context.Context, l logrus.FieldLogger, enabledStorageNames []string) error {
		enabledNames := map[string]struct{}{}
		for _, sName := range enabledStorageNames {
			enabledNames[sName] = struct{}{}
		}

		visitedPaths := map[string]bool{}

		randSrc := rand.New(rand.NewSource(time.Now().UnixNano()))
		for _, storage := range shuffledStoragesCopy(randSrc, storages) {
			if _, ok := enabledNames[storage.Name]; !ok {
				continue // storage not enabled
			}
			if visitedPaths[storage.Path] {
				continue // already visited
			}
			visitedPaths[storage.Path] = true

			l.WithField("storage_path", storage.Path).
				Info("maintenance: optimizing repos in storage")

			if err := walkReposShuffled(ctx, randSrc, l, storage.Path, storage, optimizer); err != nil {
				l.WithError(err).
					WithField("storage_path", storage.Path).
					Errorf("maintenance: unable to completely walk storage")
			}
		}
		return nil
	}
}
