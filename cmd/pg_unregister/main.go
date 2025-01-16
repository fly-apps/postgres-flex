package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
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
	machineBytes, err := base64.StdEncoding.DecodeString(encodedArg)
	if err != nil {
		return fmt.Errorf("failed to decode machine: %v", err)
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

	machineID := string(machineBytes)

	if len(machineID) != 14 {
		return fmt.Errorf("invalid machine id: %s", machineID)
	}

	member, err := node.RepMgr.MemberByNodeName(ctx, conn, machineID)
	if err != nil {
		return fmt.Errorf("failed to resolve member using %s: %s", machineID, err)
	}

	if err := node.RepMgr.UnregisterMember(*member); err != nil {
		return fmt.Errorf("failed to unregister member: %v", err)
	}

	slotName := fmt.Sprintf("repmgr_slot_%d", member.ID)
	return removeReplicationSlot(ctx, conn, slotName)
}

func removeReplicationSlot(ctx context.Context, conn *pgx.Conn, slotName string) error {
	ticker := time.NewTicker(1 * time.Second)
	timeout := time.After(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timed out trying to drop replication slot")
		case <-ticker.C:
			slot, err := admin.GetReplicationSlot(ctx, conn, slotName)
			if err != nil {
				if err == pgx.ErrNoRows {
					return nil
				}
				return fmt.Errorf("failed to get replication slot %s: %v", slotName, err)
			}

			if slot.Active {
				log.Printf("Slot %s is still active, waiting...", slotName)
				continue
			}

			if err := admin.DropReplicationSlot(ctx, conn, slotName); err != nil {
				return fmt.Errorf("failed to drop replication slot %s: %v", slotName, err)
			}

			return nil
		}
	}
}
