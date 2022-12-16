package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/flycheck"
	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v4"
)

const Port = 5500

func StartHttpServer() {
	r := chi.NewMux()

	r.Mount("/flycheck", flycheck.Handler())
	r.Mount("/commands", Handler())

	http.ListenAndServe(fmt.Sprintf(":%d", Port), r)
}

func Handler() http.Handler {
	r := chi.NewRouter()

	r.Route("/users", func(r chi.Router) {
		r.Get("/list", handleListUsers)
		r.Post("/create", handleCreateUser)
		r.Delete("/delete/{name}", handleDeleteUser)
	})

	r.Route("/databases", func(r chi.Router) {
		r.Get("/list", handleListDatabases)
		r.Get("/{name}", handleFindDatabase)
		r.Post("/create", handleCreateDatabase)
		r.Delete("/delete/{name}", handleDeleteDatabase)
	})

	r.Route("/admin", func(r chi.Router) {
		r.Get("/role", handleRole)
		// r.Get("/failover/trigger", handleFailoverTrigger)
		// r.Get("/settings/view", handleViewSettings)
		// r.Post("/settings/update", handleUpdateSettings)
	})

	return r
}

func localConnection(ctx context.Context, database string) (*pgx.Conn, func() error, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return nil, nil, err
	}

	pg, err := node.NewLocalConnection(ctx, database)
	if err != nil {
		return nil, nil, err
	}
	close := func() error {
		return pg.Close(ctx)
	}

	return pg, close, nil
}
