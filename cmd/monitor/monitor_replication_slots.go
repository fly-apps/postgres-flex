package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

func monitorReplicationSlots(ctx context.Context, node *flypg.Node) {
	inactiveSlotStatus := map[int]time.Time{}

	ticker := time.NewTicker(replicationStateMonitorFrequency)
	defer ticker.Stop()
	for range ticker.C {
		if err := replicationSlotMonitorTick(ctx, node, inactiveSlotStatus); err != nil {
			log.Printf("replicationSlotMonitorTick failed with: %s", err)
		}
	}
}

func replicationSlotMonitorTick(ctx context.Context, node *flypg.Node, inactiveSlotStatus map[int]time.Time) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		log.Printf("failed to open local connection: %s\n", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	member, err := node.RepMgr.Member(ctx, conn)
	if err != nil {
		return err
	}

	// Only monitor replication slots on the primary.
	// We need to check this per-tick as the role can change at runtime.
	if member.Role != flypg.PrimaryRoleName {
		return nil
	}

	slots, err := admin.ListReplicationSlots(ctx, conn)
	if err != nil {
		log.Printf("failed to list replication slots: %s\n", err)
	}

	for _, slot := range slots {
		if slot.Active {
			delete(inactiveSlotStatus, int(slot.MemberID))
			continue
		}

		// Log warning if inactive replication slot is holding onto more than 50MB worth of WAL.
		if slot.RetainedWalInBytes != 0 {
			retainedWalInMB := slot.RetainedWalInBytes / 1024 / 1024
			if retainedWalInMB > 50 {
				log.Printf("Warning: Inactive replication slot %s is retaining %d MB of WAL", slot.Name, retainedWalInMB)
			}
		}

		// Check to see if slot has already been registered as inactive.
		if lastSeen, ok := inactiveSlotStatus[int(slot.MemberID)]; ok {
			// TODO - Consider creating a separate threshold for when the member exists.
			// TODO - Consider being more aggressive with removing replication slots if disk capacity is at dangerous levels.
			// TODO - Make inactiveSlotRemovalThreshold configurable.

			// Remove the replication slot if it has been inactive for longer than the defined threshold
			if time.Since(lastSeen) > defaultInactiveSlotRemovalThreshold {
				log.Printf("Dropping replication slot: %s\n", slot.Name)
				if err := admin.DropReplicationSlot(ctx, conn, slot.Name); err != nil {
					log.Printf("failed to drop replication slot %s: %v\n", slot.Name, err)
					continue
				}

				delete(inactiveSlotStatus, int(slot.MemberID))
				continue
			}

			log.Printf("Replication slot %s has been inactive for %v\n", slot.Name, time.Since(lastSeen).Round(time.Second))
		} else {
			inactiveSlotStatus[int(slot.MemberID)] = time.Now()
		}
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil
}
