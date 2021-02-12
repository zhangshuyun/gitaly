package rubyserver

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func waitPing(s *Server) error {
	var err error
	for start := time.Now(); time.Since(start) < ConnectTimeout; time.Sleep(100 * time.Millisecond) {
		err = makeRequest(s)
		if err == nil {
			return nil
		}
	}
	return err
}

// This benchmark lets you see what happens when you throw a lot of
// concurrent traffic at gitaly-ruby.
func BenchmarkConcurrency(b *testing.B) {
	config.Config.Ruby.NumWorkers = 2

	s := New(config.Config)
	require.NoError(b, s.Start())
	defer s.Stop()

	// Warm-up: wait for gitaly-ruby to boot
	if err := waitPing(s); err != nil {
		b.Fatal(err)
	}

	concurrency := 100
	b.Run(fmt.Sprintf("concurrency %d", concurrency), func(b *testing.B) {
		errCh := make(chan error)
		errCount := make(chan int)
		go func() {
			count := 0
			for err := range errCh {
				b.Log(err)
				count++
			}
			errCount <- count
		}()

		wg := &sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for j := 0; j < 1000; j++ {
					err := makeRequest(s)
					if err != nil {
						errCh <- err
					}

					switch status.Code(err) {
					case codes.Unavailable:
						return
					case codes.DeadlineExceeded:
						return
					}
				}
			}()
		}

		wg.Wait()
		close(errCh)

		if count := <-errCount; count != 0 {
			b.Fatalf("received %d errors", count)
		}
	})
}

func makeRequest(s *Server) error {
	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(time.Second))
	defer cancel()

	conn, err := s.getConnection(ctx)
	if err != nil {
		return err
	}

	client := healthpb.NewHealthClient(conn)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}
