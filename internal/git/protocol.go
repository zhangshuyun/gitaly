package git

import (
	"context"
	"fmt"
	"strings"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

const (
	// ProtocolV2 is the special value used by Git clients to request protocol v2
	ProtocolV2 = "version=2"
)

var (
	gitProtocolRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_git_protocol_requests_total",
			Help: "Counter of Git protocol requests",
		},
		[]string{"grpc_service", "grpc_method", "git_protocol"},
	)
)

// RequestWithGitProtocol holds requests that respond to GitProtocol
type RequestWithGitProtocol interface {
	GetGitProtocol() string
}

// WithGitProtocol checks whether the request has Git protocol v2
// and sets this in the environment.
func WithGitProtocol(ctx context.Context, req RequestWithGitProtocol) CmdOpt {
	return func(cc *cmdCfg) error {
		cc.env = append(cc.env, gitProtocolEnv(ctx, req)...)
		return nil
	}
}

func gitProtocolEnv(ctx context.Context, req RequestWithGitProtocol) []string {
	var protocol string
	var env []string

	switch gp := req.GetGitProtocol(); gp {
	case ProtocolV2:
		env = append(env, fmt.Sprintf("GIT_PROTOCOL=%s", ProtocolV2))
		protocol = "v2"
	case "":
		protocol = "v0"
	default:
		log.Default().
			WithField("git_protocol", gp).
			Warn("invalid git protocol requested")
		protocol = "invalid"
	}

	service, method := methodFromContext(ctx)
	gitProtocolRequests.WithLabelValues(service, method, protocol).Inc()

	return env
}

func methodFromContext(ctx context.Context) (service string, method string) {
	tags := grpc_ctxtags.Extract(ctx)
	ctxValue := tags.Values()["grpc.request.fullMethod"]
	if ctxValue == nil {
		return "", ""
	}

	if s, ok := ctxValue.(string); ok {
		// Expect: "/foo.BarService/Qux"
		split := strings.Split(s, "/")
		if len(split) != 3 {
			return "", ""
		}

		return split[1], split[2]
	}

	return "", ""
}
