package smarthttp

import (
	"context"
	"io"
	"io/ioutil"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamer abstracts away the cache concrete type so that it can be override
// in tests
type streamer interface {
	GetStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (_ io.ReadCloser, err error)
	PutStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message, src io.Reader) error
}

type infoRefCache struct {
	streamer streamer
}

func newInfoRefCache(streamer streamer) infoRefCache {
	return infoRefCache{
		streamer: streamer,
	}
}

var (
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

func (c infoRefCache) tryCache(ctx context.Context, in *gitalypb.InfoRefsRequest, w io.Writer, missFn func(io.Writer) error) error {
	if len(in.GetGitConfigOptions()) > 0 ||
		len(in.GetGitProtocol()) > 0 {
		return missFn(w)
	}

	logger := ctxlogrus.Extract(ctx).WithFields(log.Fields{"service": uploadPackSvc})
	logger.Debug("Attempting to fetch cached response")
	countAttempt()

	stream, err := c.streamer.GetStream(ctx, in.GetRepository(), in)
	switch err {
	case nil:
		defer stream.Close()

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
			if err := c.streamer.PutStream(ctx, in.Repository, in, tr); err != nil {
				logger.Errorf("unable to store InfoRefsUploadPack response in cache: %q", err)

				// discard remaining bytes if caching stream
				// failed so that tee reader is not blocked
				_, err = io.Copy(ioutil.Discard, tr)
				if err != nil {
					logger.WithError(err).
						Error("unable to discard remaining InfoRefsUploadPack cache stream")
				}
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
