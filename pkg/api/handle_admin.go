package api

import (
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
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
