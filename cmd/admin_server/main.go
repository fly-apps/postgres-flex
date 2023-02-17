package main

import (
	"github.com/fly-apps/postgres-flex/internal/api"
)

func main() {
	if err := api.StartHttpServer(); err != nil {
		panic(err)
	}
}
