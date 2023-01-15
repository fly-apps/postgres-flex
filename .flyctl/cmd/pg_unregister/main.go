package main

import (
	"encoding/base64"
	"os"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-examples/postgres-ha/pkg/utils"
)

func main() {
	encodedArg := os.Args[1]
	hostnameBytes, err := base64.StdEncoding.DecodeString(encodedArg)
	if err != nil {
		utils.WriteError(err)
		sys.Exit(1)
	}

	if err := flypg.UnregisterMemberByHostname(ctx, string(hostnameBytes)); err != nil {
		utils.WriteError(err)
		sys.Exit(1)
	}

	utils.WriteOutput("Member has been succesfully unregistered")
}
