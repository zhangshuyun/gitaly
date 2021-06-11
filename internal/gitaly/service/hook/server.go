package hook

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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
	gitalypb.UnimplementedHookServiceServer
	cfg              config.Cfg
	manager          gitalyhook.Manager
	gitCmdFactory    git.CommandFactory
	packObjectsCache streamcache.Cache
}

// NewServer creates a new instance of a gRPC namespace server
func NewServer(cfg config.Cfg, manager gitalyhook.Manager, gitCmdFactory git.CommandFactory) gitalypb.HookServiceServer {
	srv := &server{
		cfg:              cfg,
		manager:          manager,
		gitCmdFactory:    gitCmdFactory,
		packObjectsCache: streamcache.NullCache{},
	}

	if poc := cfg.PackObjectsCache; poc.Enabled {
		maxAge := poc.MaxAge.Duration()
		srv.packObjectsCache = streamcache.New(
			poc.Dir,
			maxAge,
			log.Default(),
		)
		packObjectsCacheEnabled.WithLabelValues(
			poc.Dir,
			strconv.Itoa(int(maxAge.Seconds())),
		).Set(1)
	}

	return srv
}
