package backchannel

import (
	"context"
	"net"

	"google.golang.org/grpc/credentials"
)

type insecureAuthInfo struct{ credentials.CommonAuthInfo }

func (insecureAuthInfo) AuthType() string { return "insecure" }

type insecure struct{}

func (insecure) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return conn, insecureAuthInfo{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (insecure) ClientHandshake(_ context.Context, _ string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return conn, insecureAuthInfo{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (insecure) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "insecure"}
}

func (insecure) Clone() credentials.TransportCredentials { return Insecure() }

func (insecure) OverrideServerName(string) error { return nil }

// Insecure can be used in place of transport credentials when no transport security is configured.
// Its handshakes simply return the passed in connection.
//
// Similar credentials are already implemented in gRPC:
// https://github.com/grpc/grpc-go/blob/702608ffae4d03a6821b96d3e2311973d34b96dc/credentials/insecure/insecure.go
// We've reimplemented these here as upgrading our gRPC version was very involved. Once
// we've upgrade to a version that contains the insecure credentials, this implementation can be removed and
// substituted by the official implementation.
func Insecure() credentials.TransportCredentials { return insecure{} }
