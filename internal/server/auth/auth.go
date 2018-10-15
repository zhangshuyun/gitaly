package auth

import (
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	authCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_authentications",
			Help: "Counts of of Gitaly request authentication attempts",
		},
		[]string{"enforced", "status"},
	)
)

func init() {
	prometheus.MustRegister(authCount)
}

// StreamServerInterceptor checks for Gitaly bearer tokens.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return grpc_auth.StreamServerInterceptor(check)
}

// UnaryServerInterceptor checks for Gitaly bearer tokens.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return grpc_auth.UnaryServerInterceptor(check)
}

func check(ctx context.Context) (context.Context, error) {
	if len(config.Config.Auth.Token) == 0 {
		countStatus("server disabled authentication").Inc()
		return ctx, nil
	}

	err := gitalyauth.CheckToken(ctx, config.Config.Auth.Token, time.Now())
	switch status.Code(err) {
	case codes.OK:
		countStatus(okLabel()).Inc()
	case codes.Unauthenticated:
		countStatus("unauthenticated").Inc()
	case codes.PermissionDenied:
		countStatus("denied").Inc()
	default:
		countStatus("invalid").Inc()
	}

	return ctx, ifEnforced(err)
}

func ifEnforced(err error) error {
	if config.Config.Auth.Transitioning {
		return nil
	}
	return err
}

func okLabel() string {
	if config.Config.Auth.Transitioning {
		// This special value is an extra warning sign to administrators that
		// authentication is currently not enforced.
		return "would be ok"
	}
	return "ok"
}

func countStatus(status string) prometheus.Counter {
	enforced := "true"
	if config.Config.Auth.Transitioning {
		enforced = "false"
	}
	return authCount.WithLabelValues(enforced, status)
}
