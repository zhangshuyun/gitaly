package loghandler

import (
	"log"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// UnaryLogHandler handles access times and errors for unary RPC's
func UnaryLogHandler(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	logRequest(info.FullMethod, start, err)
	return resp, err
}

// StreamLogHandler handles access times and errors for stream RPC's
func StreamLogHandler(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, stream)
	logRequest(info.FullMethod, start, err)
	return err
}

func logRequest(method string, start time.Time, err error) {
	if err != nil {
		log.Printf("error: %s: %v", method, err)
	}
	log.Printf("%s %.3f", method, time.Since(start).Seconds())
}
