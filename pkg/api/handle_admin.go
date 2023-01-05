package api

import (
	"encoding/json"
	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"golang.org/x/exp/slices"
	"net/http"
)

func handleRole(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	role, err := node.RepMgr.CurrentRole(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}

	var alteredRole string
	if role == flypg.PrimaryRoleName {
		alteredRole = "primary"
	} else if role == flypg.StandbyRoleName {
		alteredRole = "replica"
	} else {
		alteredRole = "unknown"
	}

	res := &Response{Result: alteredRole}

	renderJSON(w, res, http.StatusOK)
}

func (s *Server) handleViewPostgresSettings(w http.ResponseWriter, r *http.Request) {
	internal := s.node.PGConfig.InternalConfig()
	user := s.node.PGConfig.UserConfig()

	all := map[string]interface{}{}

	for k, v := range internal {
		all[k] = v
	}
	for k, v := range user {
		all[k] = v
	}

	var in []string

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	out := map[string]interface{}{}

	for key, _ := range all {
		val, _ := all[key]
		if slices.Contains(in, key) {
			out[key] = val
		}
	}

	resp := &Response{Result: out}
	renderJSON(w, resp, http.StatusOK)
}

func (s *Server) handleViewBouncerSettings(w http.ResponseWriter, r *http.Request) {
	internal := s.node.PGBouncer.InternalConfig()
	user := s.node.PGBouncer.UserConfig()

	all := map[string]interface{}{}

	for k, v := range internal {
		all[k] = v
	}
	for k, v := range user {
		all[k] = v
	}

	var in []string

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	out := map[string]interface{}{}

	for key, _ := range all {
		val, _ := all[key]
		if slices.Contains(in, key) {
			out[key] = val
		}
	}

	resp := &Response{Result: out}
	renderJSON(w, resp, http.StatusOK)
}

func (s *Server) handleViewRepmgrSettings(w http.ResponseWriter, r *http.Request) {
	internal := s.node.RepMgr.InternalConfig()
	user := s.node.RepMgr.UserConfig()

	all := map[string]interface{}{}

	for k, v := range internal {
		all[k] = v
	}
	for k, v := range user {
		all[k] = v
	}

	var in []string

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	out := map[string]interface{}{}

	for key, _ := range all {
		val, _ := all[key]
		if slices.Contains(in, key) {
			out[key] = val
		}
	}

	resp := &Response{Result: out}
	renderJSON(w, resp, http.StatusOK)
}
