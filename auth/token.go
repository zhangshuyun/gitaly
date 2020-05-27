package gitalyauth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	TimestampThreshold = 30 * time.Second
)

var (
	errUnauthenticated = status.Errorf(codes.Unauthenticated, "authentication required")
	errDenied          = status.Errorf(codes.PermissionDenied, "permission denied")

	authErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_authentication_errors_total",
			Help: "Counts of of Gitaly request authentication errors",
		},
		[]string{"version", "error"},
	)
)

func init() {
	prometheus.MustRegister(authErrors)
}

// AuthInfo contains the authentication information coming from a request
type AuthInfo struct {
	Version       string
	SignedMessage []byte
	Message       string
}

// CheckToken checks the 'authentication' header of incoming gRPC
// metadata in ctx. It returns nil if and only if the token matches
// secret.
func CheckToken(ctx context.Context, secret string, targetTime time.Time) error {
	if len(secret) == 0 {
		panic("CheckToken: secret may not be empty")
	}

	authInfo, err := ExtractAuthInfo(ctx)
	if err != nil {
		return errUnauthenticated
	}

	if authInfo.Version == "v2" {
		if v2HmacInfoValid(authInfo.Message, authInfo.SignedMessage, []byte(secret), targetTime, TimestampThreshold) {
			return nil
		}
	}

	return errDenied
}

// ExtractAuthInfo returns an `AuthInfo` with the data extracted from `ctx`
func ExtractAuthInfo(ctx context.Context) (*AuthInfo, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")

	if err != nil {
		return nil, err
	}

	split := strings.SplitN(token, ".", 3)

	if len(split) != 3 {
		return nil, fmt.Errorf("invalid token format")
	}

	version, sig, msg := split[0], split[1], split[2]
	decodedSig, err := hex.DecodeString(sig)
	if err != nil {
		return nil, err
	}

	return &AuthInfo{Version: version, SignedMessage: decodedSig, Message: msg}, nil
}

func countV2Error(message string) { authErrors.WithLabelValues("v2", message).Inc() }

func v2HmacInfoValid(message string, signedMessage, secret []byte, targetTime time.Time, timestampThreshold time.Duration) bool {
	expectedHMAC := hmacSign(secret, message)
	if !hmac.Equal(signedMessage, expectedHMAC) {
		countV2Error("wrong hmac signature")
		return false
	}

	timestamp, err := strconv.ParseInt(message, 10, 64)
	if err != nil {
		countV2Error("cannot parse timestamp")
		return false
	}

	issuedAt := time.Unix(timestamp, 0)
	lowerBound := targetTime.Add(-timestampThreshold)
	upperBound := targetTime.Add(timestampThreshold)

	if issuedAt.Before(lowerBound) {
		countV2Error("timestamp too old")
		return false
	}

	if issuedAt.After(upperBound) {
		countV2Error("timestamp too new")
		return false
	}

	return true
}

func hmacSign(secret []byte, message string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(message))

	return mac.Sum(nil)
}
