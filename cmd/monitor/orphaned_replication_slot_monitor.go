package main

import (
	"context"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

func orphanedReplicationSlotTick(ctx context.Context, node *flypg.Node) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		fmt.Printf("failed to open local connection: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	// Query all registered standbys
	standbys, err := node.RepMgr.StandbyMembers(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query standbys: %s", err)
	}

	var orphanedSlots []admin.ReplicationSlot

	slots, err := admin.ListReplicationSlots(ctx, conn)
	if err != nil {
		fmt.Printf("failed to list replication slots: %s", err)
	}

	// An orphaned replication slot is defined as an inactive replication slot that is no longer tied to
	// and existing repmgr member.
	for _, slot := range slots {
		matchFound := false
		for _, standby := range standbys {
			if slot.MemberID == int32(standby.ID) {
				matchFound = true
			}
		}

		if !matchFound && !slot.Active {
			orphanedSlots = append(orphanedSlots, slot)
		}
	}

	if len(orphanedSlots) > 0 {
		fmt.Printf("%d orphaned replication slot(s) detected\n", len(orphanedSlots))

		for _, slot := range orphanedSlots {
			fmt.Printf("Dropping replication slot: %s\n", slot.Name)

			if err := admin.DropReplicationSlot(ctx, conn, slot.Name); err != nil {
				fmt.Printf("failed to drop replication slot %s: %v\n", slot.Name, err)
				continue
			}
		}
	}

	return nil
}
