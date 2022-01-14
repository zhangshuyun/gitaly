package rubyserver

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func waitPing(tb testing.TB, s *Server) {
	require.Eventually(tb, func() bool {
		return makeRequest(s) == nil
	}, ConnectTimeout, 100*time.Millisecond)
}

// This benchmark lets you see what happens when you throw a lot of
// concurrent traffic at gitaly-ruby.
func BenchmarkConcurrency(b *testing.B) {
	cfg := testcfg.Build(b)

	cfg.Ruby.NumWorkers = 2

	s := New(cfg, gittest.NewCommandFactory(b, cfg))
	require.NoError(b, s.Start())
	defer s.Stop()

	waitPing(b, s)

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
	ctx, cancel := testhelper.Context()
	defer cancel()

	conn, err := s.getConnection(ctx)
	if err != nil {
		return err
	}

	client := healthpb.NewHealthClient(conn)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}
