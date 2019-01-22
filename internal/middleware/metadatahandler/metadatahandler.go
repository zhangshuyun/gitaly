package metadatahandler

import (
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/prometheus/client_golang/prometheus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	requests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "service",
			Name:      "client_requests",
			Help:      "Counter of client requests received by client, call_site, auth version, and response code",
		},
		[]string{"client_name", "call_site", "auth_version", "grpc_code"},
	)
)

type metadataTags struct {
	clientName  string
	callSite    string
	authVersion string
}

func init() {
	prometheus.MustRegister(requests)
}

// CallSiteKey is the key used in ctx_tags to store the client feature
const CallSiteKey = "grpc.meta.call_site"

// ClientNameKey is the key used in ctx_tags to store the client name
const ClientNameKey = "grpc.meta.client_name"

// AuthVersionKey is the key used in ctx_tags to store the auth version
const AuthVersionKey = "grpc.meta.auth_version"

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
		clientName:  unknownValue,
		callSite:    unknownValue,
		authVersion: unknownValue,
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

	metadata = getFromMD(md, "client_name")
	if metadata != "" {
		metaTags.clientName = metadata
		tags.Set(ClientNameKey, metadata)
	}

	authInfo, _ := gitalyauth.ExtractAuthInfo(ctx)
	if authInfo != nil {
		metaTags.authVersion = authInfo.Version
		tags.Set(AuthVersionKey, authInfo.Version)
	}

	return metaTags
}

// UnaryInterceptor returns a Unary Interceptor
func UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	metaTags := addMetadataTags(ctx)

	res, err := handler(ctx, req)

	grpcCode := helper.GrpcCode(err)
	requests.WithLabelValues(metaTags.clientName, metaTags.callSite, metaTags.authVersion, grpcCode.String()).Inc()

	return res, err
}

// StreamInterceptor returns a Stream Interceptor
func StreamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := stream.Context()
	metaTags := addMetadataTags(ctx)

	err := handler(srv, stream)

	grpcCode := helper.GrpcCode(err)
	requests.WithLabelValues(metaTags.clientName, metaTags.callSite, metaTags.authVersion, grpcCode.String()).Inc()

	return err
}
