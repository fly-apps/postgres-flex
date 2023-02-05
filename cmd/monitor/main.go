package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"

	"golang.org/x/exp/maps"
)

var (
	deadMemberMonitorFrequency    = time.Minute * 5
	readonlyStateMonitorFrequency = time.Minute * 1
)

func main() {
	ctx := context.Background()
	flypgNode, err := flypg.NewNode()
	if err != nil {
		fmt.Printf("failed to reference node: %s\n", err)
		os.Exit(1)
	}

	// Dead member monitor
	go func() {
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

		ticker := time.NewTicker(deadMemberMonitorFrequency)
		defer ticker.Stop()

		fmt.Printf("Pruning every %s...\n", deadMemberRemovalThreshold)

		for range ticker.C {
			err := handleDeadMemberMonitorTick(ctx, flypgNode, seenAt, deadMemberRemovalThreshold)
			if err != nil {
				fmt.Println(err)
			}
		}
	}()

	// Readonly monitor
	ticker := time.NewTicker(readonlyStateMonitorFrequency)
	defer ticker.Stop()
	for range ticker.C {
		if err := handleReadonlyMonitorTick(ctx, flypgNode); err != nil {
			fmt.Println(err)
		}
	}

}

type readonlyStateResponse struct {
	Result bool
}

func handleReadonlyMonitorTick(ctx context.Context, node *flypg.Node) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to open local connection: %s", err)
	}
	defer conn.Close(ctx)

	member, err := node.RepMgr.Member(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query local member: %s", err)
	}

	if member.Role == flypg.PrimaryRoleName {
		return nil
	}

	primary, err := node.RepMgr.PrimaryMember(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query primary member: %s", err)
	}

	endpoint := fmt.Sprintf("http://[%s]:5500/%s", primary.Hostname, flypg.ReadOnlyStateEndpoint)
	resp, err := http.Get(endpoint)
	if err != nil {
		return fmt.Errorf("failed to query primary readonly state: %s", err)
	}
	defer resp.Body.Close()

	var state readonlyStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode result: %s", err)
	}

	if state.Result {
		if !flypg.ReadOnlyLockExists() {
			fmt.Printf("Setting connections running under %s to readonly\n", node.PrivateIP)
			if err := flypg.EnableReadonly(ctx, node); err != nil {
				return fmt.Errorf("failed to set connection under %s to readonly: %s", node.PrivateIP, err)
			}
		}
	} else {
		if !flypg.ZombieLockExists() && flypg.ReadOnlyLockExists() {
			fmt.Printf("Setting connections running under %s to read/write\n", node.PrivateIP)
			if err := flypg.DisableReadonly(ctx, node); err != nil {
				return fmt.Errorf("failed to set connections under %s read/write: %s", node.PrivateIP, err)
			}
		}
	}

	return nil
}

func handleDeadMemberMonitorTick(ctx context.Context, node *flypg.Node, seenAt map[int]time.Time, deadMemberRemovalThreshold time.Duration) error {
	// TODO - We should connect using the flypgadmin user so we can  differentiate between
	// internal admin connection usage and the actual repmgr process.
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		fmt.Printf("failed to open local connection: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	member, err := node.RepMgr.MemberByID(ctx, conn, int(node.RepMgr.ID))
	if err != nil {
		return err
	}

	if member.Role != flypg.PrimaryRoleName {
		return nil
	}

	standbys, err := node.RepMgr.StandbyMembers(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query standbys: %s", err)
	}

	for _, standby := range standbys {
		// Wrap this in a function so connections are properly closed.
		sConn, err := node.RepMgr.NewRemoteConnection(ctx, standby.Hostname)
		if err != nil {
			// TODO - Verify the exception that's getting thrown.
			if time.Since(seenAt[standby.ID]) >= deadMemberRemovalThreshold {
				if err := node.RepMgr.UnregisterMember(ctx, standby); err != nil {
					fmt.Printf("failed to unregister member %s: %v", standby.Hostname, err)
					continue
				}

				delete(seenAt, standby.ID)
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
