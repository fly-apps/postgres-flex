package server

import (
	"fmt"
	"net/http"

	"github.com/fly-apps/postgres-flex/pkg/api"
	"github.com/fly-apps/postgres-flex/pkg/flycheck"
	"github.com/go-chi/chi/v5"
)

const Port = 5500

func StartHttpServer() {
	r := chi.NewMux()

	r.Mount("/flycheck", flycheck.Handler())
	r.Mount("/commands", api.Handler())

	http.ListenAndServe(fmt.Sprintf(":%d", Port), r)
}
