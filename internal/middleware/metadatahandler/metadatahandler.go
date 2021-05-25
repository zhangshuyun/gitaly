package metadatahandler

import (
	"context"
	"strings"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	requests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "service",
			Name:      "client_requests_total",
			Help:      "Counter of client requests received by client, call_site, auth version, response code and deadline_type",
		},
		[]string{"client_name", "grpc_service", "call_site", "auth_version", "grpc_code", "deadline_type"},
	)
)

type metadataTags struct {
	clientName   string
	callSite     string
	authVersion  string
	deadlineType string
}

// CallSiteKey is the key used in ctx_tags to store the client feature
const CallSiteKey = "grpc.meta.call_site"

// ClientNameKey is the key used in ctx_tags to store the client name
const ClientNameKey = "grpc.meta.client_name"

// AuthVersionKey is the key used in ctx_tags to store the auth version
const AuthVersionKey = "grpc.meta.auth_version"

// DeadlineTypeKey is the key used in ctx_tags to store the deadline type
const DeadlineTypeKey = "grpc.meta.deadline_type"

// RemoteIPKey is the key used in ctx_tags to store the remote_ip
const RemoteIPKey = "remote_ip"

// UserIDKey is the key used in ctx_tags to store the user_id
const UserIDKey = "user_id"

// UsernameKey is the key used in ctx_tags to store the username
const UsernameKey = "username"

// CorrelationIDKey is the key used in ctx_tags to store the correlation ID
const CorrelationIDKey = "correlation_id"

// Unknown client and feature. Matches the prometheus grpc unknown value
const unknownValue = "unknown"

func getFromMD(md metadata.MD, header string) string {
	values := md[header]
	if len(values) != 1 {
		return ""
	}

	return values[0]
}

// addMetadataTags extracts metadata from the connection headers and add it to the
// ctx_tags, if it is set. Returns values appropriate for use with prometheus labels,
// using `unknown` if a value is not set
func addMetadataTags(ctx context.Context) metadataTags {
	metaTags := metadataTags{
		clientName:   unknownValue,
		callSite:     unknownValue,
		authVersion:  unknownValue,
		deadlineType: unknownValue,
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metaTags
	}

	tags := grpc_ctxtags.Extract(ctx)

	metadata := getFromMD(md, "call_site")
	if metadata != "" {
		metaTags.callSite = metadata
		tags.Set(CallSiteKey, metadata)
	}

	metadata = getFromMD(md, "deadline_type")
	_, deadlineSet := ctx.Deadline()
	if !deadlineSet {
		metaTags.deadlineType = "none"
	} else if metadata != "" {
		metaTags.deadlineType = metadata
	}

	clientName := correlation.ExtractClientNameFromContext(ctx)
	if clientName != "" {
		metaTags.clientName = clientName
		tags.Set(ClientNameKey, clientName)
	} else {
		metadata = getFromMD(md, "client_name")
		if metadata != "" {
			metaTags.clientName = metadata
			tags.Set(ClientNameKey, metadata)
		}
	}

	// Set the deadline type in the logs
	tags.Set(DeadlineTypeKey, metaTags.deadlineType)

	authInfo, _ := gitalyauth.ExtractAuthInfo(ctx)
	if authInfo != nil {
		metaTags.authVersion = authInfo.Version
		tags.Set(AuthVersionKey, authInfo.Version)
	}

	metadata = getFromMD(md, "remote_ip")
	if metadata != "" {
		tags.Set(RemoteIPKey, metadata)
	}

	metadata = getFromMD(md, "user_id")
	if metadata != "" {
		tags.Set(UserIDKey, metadata)
	}

	metadata = getFromMD(md, "username")
	if metadata != "" {
		tags.Set(UsernameKey, metadata)
	}

	// This is a stop-gap approach to logging correlation_ids
	correlationID := correlation.ExtractFromContext(ctx)
	if correlationID != "" {
		tags.Set(CorrelationIDKey, correlationID)
	}

	return metaTags
}

func extractServiceName(fullMethodName string) string {
	fullMethodName = strings.TrimPrefix(fullMethodName, "/") // remove leading slash
	if i := strings.Index(fullMethodName, "/"); i >= 0 {
		return fullMethodName[:i]
	}
	return unknownValue
}

func reportWithPrometheusLabels(metaTags metadataTags, fullMethod string, err error) {
	grpcCode := helper.GrpcCode(err)
	serviceName := extractServiceName(fullMethod)

	requests.WithLabelValues(
		metaTags.clientName,   // client_name
		serviceName,           // grpc_service
		metaTags.callSite,     // call_site
		metaTags.authVersion,  // auth_version
		grpcCode.String(),     // grpc_code
		metaTags.deadlineType, // deadline_type
	).Inc()
	grpc_prometheus.WithConstLabels(prometheus.Labels{"deadline_type": metaTags.deadlineType})
}

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	metaTags := addMetadataTags(ctx)

	res, err := handler(ctx, req)

	reportWithPrometheusLabels(metaTags, info.FullMethod, err)

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	metaTags := addMetadataTags(ctx)

	err := handler(srv, stream)

	reportWithPrometheusLabels(metaTags, info.FullMethod, err)

	return err
}
