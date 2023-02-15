package api

import (
	"encoding/json"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/go-chi/chi/v5"
)

func handleListDatabases(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	conn, close, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	dbs, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: dbs,
	}

	renderJSON(w, res, http.StatusOK)
}

func handleGetDatabase(w http.ResponseWriter, r *http.Request) {
	var (
		ctx  = r.Context()
		name = chi.URLParam(r, "name")
	)

	conn, close, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	db, err := admin.FindDatabase(ctx, conn, name)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: db,
	}

	renderJSON(w, res, http.StatusOK)
}

func handleCreateDatabase(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	conn, close, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	var input createDatabaseRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		renderErr(w, err)
		return
	}
	defer r.Body.Close()

	if err := admin.CreateDatabase(ctx, conn, input.Name); err != nil {
		renderErr(w, err)
		return
	}

	dbConn, close, err := localConnection(ctx, input.Name)
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	if err := admin.GrantCreateOnPublic(ctx, dbConn); err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{Result: true}
	renderJSON(w, res, http.StatusOK)
}

func handleDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	var (
		ctx  = r.Context()
		name = chi.URLParam(r, "name")
	)
	conn, close, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	err = admin.DeleteDatabase(ctx, conn, name)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{Result: true}

	renderJSON(w, res, http.StatusOK)
}
