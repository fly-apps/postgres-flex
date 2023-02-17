package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/go-chi/chi/v5"
)

func handleListUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	conn, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	users, err := admin.ListUsers(ctx, conn)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: users,
	}

	renderJSON(w, res, http.StatusOK)

}

func handleGetUser(w http.ResponseWriter, r *http.Request) {
	var (
		ctx  = r.Context()
		name = chi.URLParam(r, "name")
	)

	conn, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	user, err := admin.FindUser(ctx, conn, name)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: user,
	}
	renderJSON(w, res, http.StatusOK)
}

func handleCreateUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	conn, err := localConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	var input createUserRequest
	err = json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		renderErr(w, err)
		return
	}
	defer r.Body.Close()

	err = admin.CreateUser(ctx, conn, input.Username, input.Password)
	if err != nil {
		renderErr(w, err)
		return
	}

	if input.Superuser {
		err = admin.GrantSuperuser(ctx, conn, input.Username)
		if err != nil {
			renderErr(w, err)
			return
		}
	} else {
		err = admin.GrantAccess(ctx, conn, input.Username)
		if err != nil {
			renderErr(w, err)
			return
		}
	}

	res := &Response{
		Result: true,
	}

	renderJSON(w, res, http.StatusOK)
}

func handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	var (
		ctx  = r.Context()
		name = chi.URLParam(r, "name")
	)

	conn, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(r.Context())

	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		renderErr(w, fmt.Errorf("failed to list databases: %s", err))
		return
	}

	for _, database := range databases {
		dbConn, err := localConnection(r.Context(), database.Name)
		if err != nil {
			renderErr(w, err)
			return
		}
		defer dbConn.Close(r.Context())

		if err := admin.ReassignOwnership(ctx, dbConn, name, "postgres"); err != nil {
			renderErr(w, fmt.Errorf("failed to reassign ownership: %s", err))
			return
		}

		if err := admin.DropOwned(ctx, dbConn, name); err != nil {
			renderErr(w, fmt.Errorf("failed to drop remaining objects: %s", err))
			return
		}
	}

	err = admin.DropRole(ctx, conn, name)
	if err != nil {
		renderErr(w, fmt.Errorf("failed to drop role: %s", err))
		return
	}

	res := &Response{Result: true}
	renderJSON(w, res, http.StatusOK)
}
