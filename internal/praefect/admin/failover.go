package admin

import (
	"encoding/json"
	"net/http"
)

// FailoverRequest is a request for FailoverHandler
type FailoverRequest struct {
	NewPrimary string `json:"new_primary"`
}

// FailoverHandler takes a FailoverRequest and removes the current primary, and promotes a new primary
func (s *Server) FailoverHandler(w http.ResponseWriter, r *http.Request) {
	var req FailoverRequest

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	existingPrimary, err := s.d.GetPrimary()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if req.NewPrimary == existingPrimary {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("new_primary is already the primary"))
		return
	}

	var secondaries []string
	var found bool
	for _, secondary := range s.cfg.SecondaryServers {
		if secondary.Name == req.NewPrimary {
			found = true
			continue
		}
		secondaries = append(secondaries, secondary.Name)
	}

	if !found {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("new_primary is already the primary"))
		return
	}

	s.coordinator.Lock()
	defer s.coordinator.Unlock()

	if err := s.d.SetPrimary(req.NewPrimary); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := s.coordinator.UnregisterNode(existingPrimary); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}
