package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/jackc/pgx/v4"

	"golang.org/x/exp/maps"
)

var (
	monitorFrequency = time.Minute * 5
)

func main() {
	ctx := context.Background()
	flypgNode, err := flypg.NewNode()
	if err != nil {
		fmt.Printf("failed to reference node: %s\n", err)
		os.Exit(1)
	}

	// TODO - We should connect using the flypgadmin user so we can  differentiate between
	// internal admin connection usage and the actual repmgr process.
	conn, err := flypgNode.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		fmt.Printf("failed to open local connection: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	internal, err := flypg.ReadFromFile("/data/flypg.internal.conf")
	if err != nil {
		fmt.Printf("failed to open config: %s\n", err)
		os.Exit(1)
	}

	user, err := flypg.ReadFromFile("/data/flypg.user.conf")
	if err != nil {
		fmt.Printf("failed to open config: %s\n", err)
		os.Exit(1)
	}

	maps.Copy(user, internal)

	deadMemberRemovalThreshold, err := time.ParseDuration(fmt.Sprint(internal["standby_clean_interval"]))
	if err != nil {
		fmt.Printf(fmt.Sprintf("Failed to parse config: %s", err))
		os.Exit(1)
	}

	seenAt := map[int]time.Time{}

	ticker := time.NewTicker(monitorFrequency)
	defer ticker.Stop()

	fmt.Printf("Pruning every %s...\n", deadMemberRemovalThreshold)

	for {
		select {
		case <-ticker.C:
			role, err := flypgNode.RepMgr.CurrentRole(ctx, conn)
			if err != nil {
				fmt.Printf("Failed to check role: %s\n", err)
				continue
			}

			if role != flypg.PrimaryRoleName {
				continue
			}

			standbys, err := flypgNode.RepMgr.Standbys(ctx, conn)
			if err != nil {
				fmt.Printf("Failed to query standbys: %s\n", err)
				continue
			}

			for _, standby := range standbys {
				newConn, err := flypgNode.RepMgr.NewRemoteConnection(ctx, standby.Ip)
				defer newConn.Close(ctx)
				if err != nil {
					// TODO - Verify the exception that's getting thrown.
					if time.Now().Sub(seenAt[standby.Id]) >= deadMemberRemovalThreshold {
						if err := flypgNode.UnregisterMemberByID(ctx, int32(standby.Id)); err != nil {
							fmt.Printf("failed to unregister member %d: %v\n", standby.Id, err.Error())
							continue
						}

						delete(seenAt, standby.Id)
					}

					continue
				}

				seenAt[standby.Id] = time.Now()
			}

			removeOrphanedReplicationSlots(ctx, conn, standbys)
		}
	}
}

func removeOrphanedReplicationSlots(ctx context.Context, conn *pgx.Conn, standbys []flypg.Standby) {
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
			if slot.MemberID == int32(standby.Id) {
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
}
