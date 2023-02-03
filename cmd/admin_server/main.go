package main

import (
	"github.com/fly-apps/postgres-flex/internal/api"
	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func main() {
	node, err := flypg.NewNode()
	if err != nil {
		panic(err)
	}

	api.StartHttpServer(node)
}
