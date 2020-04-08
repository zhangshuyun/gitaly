package smarthttp

import (
	"context"
	"io"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	infoRefCache = cache.NewStreamDB(cache.LeaseKeyer{})

	// prometheus counters
	cacheAttemptTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_inforef_cache_attempt_total",
			Help: "Total number of smarthttp info-ref RPCs accessing the cache",
		},
	)
	hitMissTotals = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_inforef_cache_hit_miss_total",
			Help: "Total number of smarthttp info-ref RPCs accessing the cache",
		},
		[]string{"type"},
	)

	// counter functions are package vars to enable easy overriding for tests
	countAttempt = func() { cacheAttemptTotal.Inc() }
	countHit     = func() { hitMissTotals.WithLabelValues("hit").Inc() }
	countMiss    = func() { hitMissTotals.WithLabelValues("miss").Inc() }
	countErr     = func() { hitMissTotals.WithLabelValues("err").Inc() }
)

func init() {
	prometheus.MustRegister(cacheAttemptTotal)
	prometheus.MustRegister(hitMissTotals)
}

// UploadPackCacheFeatureFlagKey enables cache usage in InfoRefsUploadPack RPC
const UploadPackCacheFeatureFlagKey = "inforef-uploadpack-cache"

func tryCache(ctx context.Context, in *gitalypb.InfoRefsRequest, w io.Writer, missFn func(io.Writer) error) error {
	if !featureflag.IsEnabled(ctx, UploadPackCacheFeatureFlagKey) ||
		!featureflag.IsEnabled(ctx, featureflag.CacheInvalidator) ||
		len(in.GetGitConfigOptions()) > 0 ||
		len(in.GetGitProtocol()) > 0 {
		return missFn(w)
	}

	logger := ctxlogrus.Extract(ctx).WithFields(log.Fields{"service": uploadPackSvc})
	logger.Debug("Attempting to fetch cached response")
	countAttempt()

	stream, err := infoRefCache.GetStream(ctx, in.GetRepository(), in)
	switch err {
	case nil:
		countHit()
		logger.Info("cache hit for UploadPack response")

		if _, err := io.Copy(w, stream); err != nil {
			return status.Errorf(codes.Internal, "GetInfoRefs: cache copy: %v", err)
		}

		return nil

	case cache.ErrReqNotFound:
		countMiss()
		logger.Info("cache miss for InfoRefsUploadPack response")

		var wg sync.WaitGroup
		defer wg.Wait()

		pr, pw := io.Pipe()

		wg.Add(1)
		go func() {
			defer wg.Done()

			tr := io.TeeReader(pr, w)
			if err := infoRefCache.PutStream(ctx, in.Repository, in, tr); err != nil {
				logger.Errorf("unable to store InfoRefsUploadPack response in cache: %q", err)
			}
		}()

		err = missFn(pw)
		_ = pw.CloseWithError(err) // always returns nil
		return err

	default:
		countErr()
		logger.Infof("unable to fetch cached response: %q", err)

		return missFn(w)
	}
}
