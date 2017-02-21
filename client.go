package main

// go run client.go

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

const (
	// Must match the subject (CN) of the server's certificate
	serverName = "gitaly"
)

func main() {
	// This is a self-signed certificate used by both the client and the
	// server. We use it here both as the client certificate and as the CA
	// root for the server's certificate. The server does the opposite. The
	// only special thing we do is that here in the client we override the
	// ServerName attribute of the TLS configuration. This prevents us from
	// having to disable peer verification in the client.
	cert, err := tls.LoadX509KeyPair("gitaly.crt", "gitaly.key")
	if err != nil {
		log.Fatal(err)
	}

	// Load server CA cert
	caCert, err := ioutil.ReadFile("gitaly.crt")
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		ServerName:   serverName,
	}
	tlsConfig.BuildNameToCertificate()

	creds := credentials.NewTLS(tlsConfig)
	conn, err := grpc.Dial("localhost:8877", grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	client := pb.NewNotificationsClient(conn)

	repo := &pb.Repository{Path: "foo"}
	rpcRequest := &pb.PostReceiveRequest{Repository: repo}

	_, err = client.PostReceive(context.Background(), rpcRequest)
	log.Print(err)
}
