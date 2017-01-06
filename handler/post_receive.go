package handler

import (
	"net/http"
)

func PostReceive(w http.ResponseWriter, r *http.Request) {
	// TODO: Invalidate info-refs cache. For now, just return 200
	w.WriteHeader(http.StatusOK)
}
