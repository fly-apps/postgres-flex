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
			if err := handleTick(ctx, flypgNode, seenAt, deadMemberRemovalThreshold); err != nil {
				fmt.Println(err)
			}
		}
	}
}

func handleTick(ctx context.Context, node *flypg.Node, seenAt map[int]time.Time, deadMemberRemovalThreshold time.Duration) error {
	// TODO - We should connect using the flypgadmin user so we can  differentiate between
	// internal admin connection usage and the actual repmgr process.
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		fmt.Printf("failed to open local connection: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	member, err := node.RepMgr.ResolveMemberByID(ctx, conn, int(node.RepMgr.ID))
	if err != nil {
		return err
	}

	if member.Role != flypg.PrimaryRoleName {
		return nil
	}

	standbys, err := node.RepMgr.ResolveStandbys(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query standbys: %s", err)
	}

	for _, standby := range standbys {
		// Wrap this in a function so connections are properly closed.
		sConn, err := node.RepMgr.NewRemoteConnection(ctx, standby.Hostname)
		if err != nil {
			// TODO - Verify the exception that's getting thrown.
			if time.Now().Sub(seenAt[standby.ID]) >= deadMemberRemovalThreshold {
				if err := node.UnregisterMemberByHostname(ctx, standby.Hostname); err != nil {
					fmt.Printf("failed to unregister member %s: %v", standby.Hostname, err)
					continue
				}

				delete(seenAt, int(standby.ID))
			}

			continue
		}
		defer sConn.Close(ctx)

		seenAt[standby.ID] = time.Now()
	}

	removeOrphanedReplicationSlots(ctx, conn, standbys)

	return nil
}

func removeOrphanedReplicationSlots(ctx context.Context, conn *pgx.Conn, standbys []flypg.Member) {
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
}
