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
	ctx := context.Background()

	if err := processUnregistration(ctx); err != nil {
		utils.WriteError(err)
		os.Exit(1)
	}

	utils.WriteOutput("Member has been succesfully unregistered", "")
}

func processUnregistration(ctx context.Context) error {
	encodedArg := os.Args[1]
	hostnameBytes, err := base64.StdEncoding.DecodeString(encodedArg)
	if err != nil {
		return fmt.Errorf("failed to decode hostname: %v", err)
	}

	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("faied to initialize node: %s", err)
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to local db: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	member, err := node.RepMgr.MemberByHostname(ctx, conn, string(hostnameBytes))
	if err != nil {
		return fmt.Errorf("failed to resolve member: %s", err)
	}

	if err := node.RepMgr.UnregisterMember(*member); err != nil {
		return fmt.Errorf("failed to unregister member: %v", err)
	}

	return nil
}
