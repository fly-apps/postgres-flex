package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/flycheck"
	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

const Port = 5500

type Server struct {
	node *flypg.Node
}

func StartHttpServer(node *flypg.Node) {
	server := &Server{node: node}
	r := chi.NewMux()

	r.Mount("/flycheck", flycheck.Handler())
	r.Mount("/commands", server.Handler())

	http.ListenAndServe(fmt.Sprintf(":%d", Port), r)
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()

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
		r.Get("/settings/view/postgres", s.handleViewPostgresSettings)
		r.Get("/settings/view/repmgr", s.handleViewRepmgrSettings)
		r.Post("/settings/update/postgres", s.handleUpdatePostgresSettings)
		r.Post("/settings/apply", s.handleApplyConfig)
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
