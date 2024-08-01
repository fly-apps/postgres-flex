package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
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
		return fmt.Errorf("failed to initialize node: %s", err)
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

	slots, err := admin.ListReplicationSlots(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to list replication slots: %v", err)
	}

	targetSlot := fmt.Sprintf("repmgr_slot_%d", member.ID)
	for _, slot := range slots {
		if slot.Name == targetSlot {
			if err := admin.DropReplicationSlot(ctx, conn, targetSlot); err != nil {
				return fmt.Errorf("failed to drop replication slot: %v", err)
			}
			break
		}
	}

	return nil
}
