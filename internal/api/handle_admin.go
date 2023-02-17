package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"golang.org/x/exp/slices"
)

func handleReadonlyState(w http.ResponseWriter, _ *http.Request) {
	res := &Response{
		Result: false,
	}

	if flypg.ReadOnlyLockExists() || flypg.ZombieLockExists() {
		res.Result = true
	}

	renderJSON(w, res, http.StatusOK)
}

func handleHaproxyRestart(w http.ResponseWriter, _ *http.Request) {
	if err := flypg.RestartHaproxy(); err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{
		Result: true,
	}

	renderJSON(w, res, http.StatusOK)
}

func handleEnableReadonly(w http.ResponseWriter, r *http.Request) {
	res := &Response{
		Result: true,
	}

	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	if err := flypg.EnableReadonly(r.Context(), node); err != nil {
		renderErr(w, err)
		return
	}

	renderJSON(w, res, http.StatusOK)
}

func handleDisableReadonly(w http.ResponseWriter, r *http.Request) {
	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{
		Result: false,
	}

	if err := flypg.DisableReadonly(r.Context(), node); err != nil {
		renderErr(w, err)
		return
	}

	res.Result = true

	renderJSON(w, res, http.StatusOK)
}

func handleRole(w http.ResponseWriter, r *http.Request) {
	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

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
	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	consul, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	user, err := flypg.ReadFromFile(s.node.PGConfig.UserConfigFile())
	if err != nil {
		renderErr(w, err)
		return
	}

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

	for k := range user {
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
	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

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
	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	all, err := s.node.PGConfig.CurrentConfig()
	if err != nil {
		renderErr(w, err)
		return
	}

	var in []string

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	var out []admin.PGSetting

	for key := range all {
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

func (s *Server) handleViewRepmgrSettings(w http.ResponseWriter, r *http.Request) {
	all, err := s.node.RepMgr.CurrentConfig()
	if err != nil {
		renderErr(w, err)
		return
	}

	var in []string

	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		renderErr(w, err)
		return
	}

	out := map[string]interface{}{}

	for key := range all {
		val := all[key]
		if slices.Contains(in, key) {
			out[key] = val
		}
	}

	resp := &Response{Result: out}
	renderJSON(w, resp, http.StatusOK)
}
