package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
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
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to local db: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	member, err := node.RepMgr.MemberByHostname(ctx, conn, string(hostnameBytes))
	if errors.Is(err, pgx.ErrNoRows) {
		// for historical reasons, flyctl passes in the 6pn as the hostname
		member, err = node.RepMgr.MemberBy6PN(ctx, conn, string(hostnameBytes))
		if err != nil {
			return fmt.Errorf("failed to resolve member by hostname and 6pn: %s", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to resolve member: %s", err)
	}

	if err := node.RepMgr.UnregisterMember(*member); err != nil {
		return fmt.Errorf("failed to unregister member: %v", err)
	}

	return nil
}
