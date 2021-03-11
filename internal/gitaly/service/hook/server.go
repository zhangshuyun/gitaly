package hook

import (
	"path/filepath"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	packObjectsCacheEnabled = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitaly_pack_objects_cache_enabled",
			Help: "If set to 1, indicates that the cache for PackObjectsHook has been enabled in this process",
		},
		[]string{"dir", "max_age"},
	)
)

type server struct {
	cfg              config.Cfg
	manager          gitalyhook.Manager
	gitCmdFactory    git.CommandFactory
	packObjectsCache streamcache.Cache
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(cfg config.Cfg, manager gitalyhook.Manager, gitCmdFactory git.CommandFactory) gitalypb.HookServiceServer {
	srv := &server{
		cfg:           cfg,
		manager:       manager,
		gitCmdFactory: gitCmdFactory,
	}

	if len(cfg.Storages) > 0 {
		// TODO make the cache configurable via config.toml.
		// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/921
		//
		// Unless someone enables the gitaly_upload_pack_gitaly_hooks feature
		// flag, PackObjectsHook is never called, and srv.packObjectsCache is
		// never accessed.
		//
		// While we are still evaluating the design of the cache, we do not want
		// to commit to a configuration "interface" yet.

		// On gitlab.com, all storages point to the same directory so it does not
		// matter which one we pick. Our current plan is to store cache data on
		// the same filesystem used for persistent repository storage. Also see
		// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/792 for
		// discussion.
		dir := filepath.Join(tempdir.StreamCacheDir(cfg.Storages[0]), "PackObjectsHook")

		// 5 minutes appears to be a reasonable number for deduplicating CI clone
		// waves. See
		// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/872 for
		// discussion.
		maxAge := 5 * time.Minute

		logger := log.Default()
		if cache, err := streamcache.New(dir, maxAge, logger); err != nil {
			logger.WithError(err).Error("instantiate PackObjectsHook cache")
		} else {
			srv.packObjectsCache = cache
			packObjectsCacheEnabled.WithLabelValues(dir, strconv.Itoa(int(maxAge.Seconds()))).Set(1)
		}
	}

	return srv
}
