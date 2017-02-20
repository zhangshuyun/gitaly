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

func main() {

	//	// Load client cert. yes 'gitaly-server' is also the client certificate.
	cert, err := tls.LoadX509KeyPair("_ca/certs/gitaly-server.pem", "_ca/certs/gitaly-server-key.pem")
	if err != nil {
		log.Fatal(err)
	}

	// Load server CA cert
	caCert, err := ioutil.ReadFile("_ca/certs/ca.pem")
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		ServerName:   "gitaly-server",
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
