package main

import (
	"context"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

func monitorReplicationSlots(ctx context.Context, node *flypg.Node) error {
	inactiveSlotStatus := map[int]time.Time{}

	ticker := time.NewTicker(replicationStateMonitorFrequency)
	defer ticker.Stop()
	for range ticker.C {
		if err := replicationSlotMonitorTick(ctx, node, inactiveSlotStatus); err != nil {
			log.Printf("replicationSlotMonitorTick failed with: %s", err)
		}
	}

	return nil
}

func replicationSlotMonitorTick(ctx context.Context, node *flypg.Node, inactiveSlotStatus map[int]time.Time) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		log.Printf("failed to open local connection: %s\n", err)
	}
	defer conn.Close(ctx)

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
		// Cleanup inactive replication slots so we don't inadvertantly run ourselves out of disk space.
		if !slot.Active {
			if slot.RetainedWalInBytes != 0 {
				retainedWalInMB := slot.RetainedWalInBytes / 1024 / 1024
				log.Printf("warning: inactive replication slot %s is retaining %d MB of WAL", slot.Name, retainedWalInMB)
			}

			// Check to see if slot has already been registered as inactive.
			if lastSeen, ok := inactiveSlotStatus[int(slot.MemberID)]; ok {
				// TODO - Consider creating a separate threshold for when the member exists.
				// TODO - Consider being more aggressive with removing replication slots if disk
				// capacity is at dangerous levels.

				// Remove the replication slot if it has been inactive for longer than the defined threshold
				if time.Since(lastSeen) > defaultInactiveSlotRemovalThreshold {
					log.Printf("Dropping replication slot: %s\n", slot.Name)
					if err := admin.DropReplicationSlot(ctx, conn, slot.Name); err != nil {
						log.Printf("failed to drop replication slot %s: %v\n", slot.Name, err)
						continue
					}
					delete(inactiveSlotStatus, int(slot.MemberID))
				}
			} else {
				inactiveSlotStatus[int(slot.MemberID)] = time.Now()
			}
		} else {
			delete(inactiveSlotStatus, int(slot.MemberID))
		}
	}

	return nil
}
