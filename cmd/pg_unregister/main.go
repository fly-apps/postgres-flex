package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

func main() {
	encodedArg := os.Args[1]
	hostnameBytes, err := base64.StdEncoding.DecodeString(encodedArg)
	if err != nil {
		utils.WriteError(fmt.Errorf("failed to decode hostname: %v", err))
		os.Exit(1)
		return
	}

	ctx := context.Background()

	node, err := flypg.NewNode()
	if err != nil {
		utils.WriteError(err)
		os.Exit(1)
		return
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		utils.WriteError(fmt.Errorf("failed to connect to local db: %s", err))
		os.Exit(1)
		return
	}

	member, err := node.RepMgr.MemberByHostname(ctx, conn, string(hostnameBytes))
	if err != nil {
		utils.WriteError(fmt.Errorf("failed to resolve member: %s", err))
		os.Exit(1)
		return
	}

	if err := node.RepMgr.UnregisterMember(ctx, *member); err != nil {
		utils.WriteError(fmt.Errorf("failed to unregister member: %v", err))
		os.Exit(1)
		return
	}

	utils.WriteOutput("Member has been succesfully unregistered", "")
}
