package admin

import (
	"fmt"
	"net/http"

	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
)

// Server is the struct for the admin server
type Server struct {
	d praefect.Datastore
	*http.Server
	cfg         config.Config
	coordinator *praefect.Coordinator
}

// NewServer creates a new admin server
func NewServer(port int, d praefect.Datastore, coordinator *praefect.Coordinator, c config.Config) *Server {
	s := &Server{
		d: d,
		Server: &http.Server{
			Addr: fmt.Sprintf("0.0.0.0:%d", port),
		},
		cfg:         c,
		coordinator: coordinator,
	}

	router := http.NewServeMux()
	router.Handle("/failover", http.HandlerFunc(s.FailoverHandler))

	s.Handler = router

	return s
}
