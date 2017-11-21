package server

import (
	"net/http"
	"net/http/pprof"
)

// AddPprofHandlers added profiling endpoints
func AddPprofHandlers(serveMux *http.ServeMux) {
	// Register pprof handlers
	serveMux.HandleFunc("/debug/pprof/", pprof.Index)
	serveMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serveMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serveMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serveMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
