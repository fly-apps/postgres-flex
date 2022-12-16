package api

import (
	"encoding/json"
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/go-chi/chi/v5"
)

func handleListDatabases(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	dbs, err := admin.ListDatabases(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: dbs,
	}

	renderJSON(w, res, http.StatusOK)
}

func handleFindDatabase(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	name := chi.URLParam(r, "name")

	db, err := admin.FindDatabase(r.Context(), conn, name)
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
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	input := createDatabaseRequest{}

	err = json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		renderErr(w, err)
		return
	}
	defer r.Body.Close()

	_, err = admin.CreateDatabase(conn, input.Name, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}

	res := &Response{Result: true}

	renderJSON(w, res, http.StatusOK)
}

func handleDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	name := chi.URLParam(r, "name")

	err = admin.DeleteDatabase(r.Context(), conn, name)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{Result: true}

	renderJSON(w, res, http.StatusOK)
}
