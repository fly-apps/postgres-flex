package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flycheck"
	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const Port = 5500

func StartHttpServer() error {
	log.SetFlags(0)

	r := chi.NewMux()
	r.Mount("/flycheck", flycheck.Handler())
	r.Mount("/commands", Handler())

	server := &http.Server{
		Handler:           r,
		Addr:              fmt.Sprintf(":%v", Port),
		ReadHeaderTimeout: 3 * time.Second,
	}

	return server.ListenAndServe()
}

func Handler() http.Handler {
	r := chi.NewRouter()
	r.Route("/events", func(r chi.Router) {
		r.Post("/process", handleEvent)
	})

	r.Route("/users", func(r chi.Router) {
		r.Get("/{name}", handleGetUser)
		r.Get("/list", handleListUsers)
		r.Post("/create", handleCreateUser)
		r.Delete("/delete/{name}", handleDeleteUser)
	})

	r.Route("/databases", func(r chi.Router) {
		r.Get("/list", handleListDatabases)
		r.Get("/{name}", handleGetDatabase)
		r.Post("/create", handleCreateDatabase)
		r.Delete("/delete/{name}", handleDeleteDatabase)
	})

	r.Route("/admin", func(r chi.Router) {
		r.Get("/readonly/enable", handleEnableReadonly)
		r.Get("/readonly/disable", handleDisableReadonly)
		r.Get("/readonly/state", handleReadonlyState)
		r.Get("/haproxy/restart", handleHaproxyRestart)

		r.Get("/role", handleRole)
		r.Get("/settings/view/postgres", handleViewPostgresSettings)
		r.Get("/settings/view/repmgr", handleViewRepmgrSettings)
		r.Get("/settings/view/barman", handleViewBarmanSettings)

		r.Post("/settings/update/postgres", handleUpdatePostgresSettings)
		r.Post("/settings/update/barman", handleUpdateBarmanSettings)

		r.Post("/settings/apply", handleApplyConfig)
	})

	return r
}

func localConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return nil, err
	}

	pg, err := node.NewLocalConnection(ctx, database, node.OperatorCredentials)
	if err != nil {
		return nil, err
	}

	return pg, nil
}
