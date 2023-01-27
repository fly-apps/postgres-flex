package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"golang.org/x/exp/slices"
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

	member, err := node.RepMgr.Member(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}

	var alteredRole string

	switch member.Role {
	case flypg.PrimaryRoleName:
		alteredRole = "primary"
	case flypg.StandbyRoleName:
		alteredRole = "replica"
	default:
		alteredRole = "unknown"
	}

	res := &Response{Result: alteredRole}

	renderJSON(w, res, http.StatusOK)
}

type SettingsUpdate struct {
	Message         string `json:"message"`
	RestartRequired bool   `json:"restart_required"`
}

func (s *Server) handleUpdatePostgresSettings(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	consul, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	user := s.node.PGConfig.UserConfig()

	var in map[string]interface{}

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	for k, v := range in {
		exists, err := admin.SettingExists(r.Context(), conn, k)
		if err != nil {
			renderErr(w, err)
			return
		}
		if !exists {
			renderErr(w, fmt.Errorf("invalid config option: %s", k))
			return
		}
		user[k] = v
	}

	s.node.PGConfig.SetUserConfig(user)

	var requiresRestart []string

	for k, _ := range user {
		restart, err := admin.SettingRequiresRestart(r.Context(), conn, k)
		if err != nil {
			renderErr(w, err)
			return
		}
		if restart {
			requiresRestart = append(requiresRestart, k)
		}
	}

	res := &Response{Result: SettingsUpdate{
		Message:         "Updated",
		RestartRequired: false,
	}}

	if len(requiresRestart) > 0 {
		res = &Response{Result: SettingsUpdate{
			Message:         fmt.Sprintf("Updated, but settings %s need a restart to apply", strings.Join(requiresRestart, ", ")),
			RestartRequired: true,
		}}
	}

	err = flypg.PushUserConfig(s.node.PGConfig, consul)
	if err != nil {
		renderErr(w, err)
		return
	}

	renderJSON(w, res, http.StatusOK)
}

func (s *Server) handleApplyConfig(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	consul, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	err = flypg.SyncUserConfig(s.node.PGConfig, consul)
	if err != nil {
		renderErr(w, err)
		return
	}

	err = admin.ReloadPostgresConfig(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}
}

type PGSettingsResponse struct {
	Settings []admin.PGSetting `json:"settings"`
}

func (s *Server) handleViewPostgresSettings(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}

	defer close()
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

	var out []admin.PGSetting

	for key, _ := range all {
		if slices.Contains(in, key) {
			setting, err := admin.GetSetting(r.Context(), conn, key)
			if err != nil {
				renderErr(w, err)
				return
			}
			out = append(out, *setting)
		}
	}

	resp := &Response{Result: PGSettingsResponse{Settings: out}}
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
