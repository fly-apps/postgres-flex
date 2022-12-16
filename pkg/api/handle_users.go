package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/go-chi/chi/v5"
)

func handleListUsers(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	users, err := admin.ListUsers(r.Context(), conn)
	if err != nil {
		renderErr(w, err)
		return
	}
	res := &Response{
		Result: users,
	}

	renderJSON(w, res, http.StatusOK)

}

func handleFindUser(w http.ResponseWriter, r *http.Request) {
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	name := chi.URLParam(r, "name")

	user, err := admin.FindUser(r.Context(), conn, name)
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
	conn, close, err := localConnection(r.Context(), "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer close()

	var input createUserRequest

	err = json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		renderErr(w, err)
		return
	}
	defer r.Body.Close()

	err = admin.CreateUser(r.Context(), conn, input.Username, input.Password)
	if err != nil {
		renderErr(w, err)
		return
	}

	err = admin.GrantAccess(r.Context(), conn, input.Username)
	if err != nil {
		renderErr(w, err)
		return
	}

	if input.Superuser {
		err = admin.GrantSuperuser(r.Context(), conn, input.Username)
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

	node, err := flypg.NewNode()
	if err != nil {
		renderErr(w, err)
		return
	}

	conn, err := node.NewLocalConnection(ctx, "postgres")
	if err != nil {
		renderErr(w, err)
		return
	}
	defer conn.Close(ctx)

	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		renderErr(w, fmt.Errorf("failed to list databases: %s", err))
		return
	}

	for _, database := range databases {
		dbConn, err := node.NewLocalConnection(r.Context(), database.Name)
		if err != nil {
			renderErr(w, err)
			return
		}
		defer dbConn.Close(ctx)

		fmt.Printf("Reassigning ownership to %s to postgres on %s\n", name, database.Name)
		if err := admin.ReassignOwnership(ctx, dbConn, name, "postgres"); err != nil {
			renderErr(w, fmt.Errorf("failed to reassign ownership: %s", err))
			return
		}

		fmt.Printf("Dropping owned by %s on %s\n", name, database.Name)
		if err := admin.DropOwned(ctx, dbConn, name); err != nil {
			renderErr(w, fmt.Errorf("failed to drop remaining objects: %s", err))
			return
		}
	}

	fmt.Printf("Dropping role %s\n", name)
	err = admin.DropRole(ctx, conn, name)
	if err != nil {
		renderErr(w, fmt.Errorf("failed to drop role: %s", err))
		return
	}

	res := &Response{Result: true}
	renderJSON(w, res, http.StatusOK)
}
