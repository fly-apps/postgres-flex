package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	conn, err := localConnection(r.Context(), "repmgr")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer func() { _ = conn.Close(r.Context()) }()

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

func handleUpdatePostgresSettings(w http.ResponseWriter, r *http.Request) {
	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer func() { _ = conn.Close(r.Context()) }()

	consul, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	cfg, err := flypg.ReadFromFile(node.PGConfig.UserConfigFile())
	if err != nil {
		renderErr(w, err)
		return
	}

	var requestedChanges map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&requestedChanges); err != nil {
		renderErr(w, err)
		return
	}

	// Logistical PG setting validations.
	requestedChanges, err = node.PGConfig.Validate(r.Context(), conn, requestedChanges)
	if err != nil {
		renderErr(w, err)
		return
	}

	for k, v := range requestedChanges {
		cfg[k] = v
	}

	node.PGConfig.SetUserConfig(cfg)

	var requiresRestart []string

	for k := range cfg {
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

	err = flypg.PushUserConfig(&node.PGConfig, consul)
	if err != nil {
		renderErr(w, err)
		return
	}

	renderJSON(w, res, http.StatusOK)
}

func handleApplyConfig(w http.ResponseWriter, r *http.Request) {
	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer func() { _ = conn.Close(r.Context()) }()

	consul, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	err = flypg.SyncUserConfig(&node.PGConfig, consul)
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

func handleViewPostgresSettings(w http.ResponseWriter, r *http.Request) {
	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer func() { _ = conn.Close(r.Context()) }()

	var requestedSettings []string
	if err := json.NewDecoder(r.Body).Decode(&requestedSettings); err != nil {
		renderErr(w, err)
		return
	}

	var settings []admin.PGSetting
	for _, key := range requestedSettings {
		setting, err := admin.GetSetting(r.Context(), conn, key)
		if err != nil {
			renderErr(w, err)
			return
		}
		settings = append(settings, *setting)
	}

	resp := &Response{
		Result: PGSettingsResponse{
			Settings: settings,
		},
	}
	renderJSON(w, resp, http.StatusOK)
}

func handleViewRepmgrSettings(w http.ResponseWriter, r *http.Request) {
	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	all, err := node.RepMgr.CurrentConfig()
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

func handleViewBarmanSettings(w http.ResponseWriter, _ *http.Request) {
	if os.Getenv("S3_ARCHIVE_CONFIG") == "" {
		renderErr(w, fmt.Errorf("barman is not enabled"))
		return
	}

	store, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		renderErr(w, err)
		return
	}

	if err := barman.LoadConfig(flypg.DefaultBarmanConfigDir); err != nil {
		renderErr(w, err)
		return
	}

	all, err := barman.CurrentConfig()
	if err != nil {
		renderErr(w, err)
		return
	}

	resp := &Response{Result: all}
	renderJSON(w, resp, http.StatusOK)
}

func handleUpdateBarmanSettings(w http.ResponseWriter, r *http.Request) {
	if os.Getenv("S3_ARCHIVE_CONFIG") == "" {
		renderErr(w, fmt.Errorf("barman is not enabled"))
		return
	}

	store, err := state.NewStore()
	if err != nil {
		renderErr(w, err)
		return
	}

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		renderErr(w, err)
		return
	}

	if err := barman.LoadConfig(flypg.DefaultBarmanConfigDir); err != nil {
		renderErr(w, err)
		return
	}

	cfg, err := flypg.ReadFromFile(barman.UserConfigFile())
	if err != nil {
		renderErr(w, err)
		return
	}

	var requestedChanges map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&requestedChanges); err != nil {
		renderErr(w, err)
		return
	}

	if err := barman.Validate(requestedChanges); err != nil {
		renderErr(w, err)
		return
	}

	for k, v := range requestedChanges {
		cfg[k] = v
	}

	barman.SetUserConfig(cfg)

	if err := flypg.PushUserConfig(barman, store); err != nil {
		renderErr(w, err)
		return
	}

	if err := flypg.SyncUserConfig(barman, store); err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{Result: SettingsUpdate{
		Message:         "Updated",
		RestartRequired: true,
	}}

	renderJSON(w, res, http.StatusOK)
}
