package commands

import (
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
)

func handleRole(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context())
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	role, err := admin.ResolveRole(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{Result: role}

	renderJSON(w, res, http.StatusOK)
}
