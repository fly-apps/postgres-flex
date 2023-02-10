package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"golang.org/x/exp/maps"
)

func monitorDeadMembers(ctx context.Context, node *flypg.Node) error {
	internal, err := flypg.ReadFromFile("/data/flypg.internal.conf")
	if err != nil {
		return fmt.Errorf("failed to open config: %s", err)
	}

	user, err := flypg.ReadFromFile("/data/flypg.user.conf")
	if err != nil {
		return fmt.Errorf("failed to open config: %s", err)
	}

	maps.Copy(user, internal)

	removalThreshold := defaultDeadMemberRemovalThreshold

	if internal["deadMemberRemovalThreshold"] != "" {
		removalThreshold, err = time.ParseDuration(fmt.Sprint(internal["deadMemberRemovalThreshold"]))
		if err != nil {
			log.Printf("failed to parse deadMemberRemovalThreshold: %s\n", err)
		}
	}

	seenAt := map[int]time.Time{}

	ticker := time.NewTicker(deadMemberMonitorFrequency)
	defer ticker.Stop()

	log.Printf("Pruning every %s...\n", removalThreshold)

	for range ticker.C {
		err := deadMemberMonitorTick(ctx, node, seenAt, removalThreshold)
		if err != nil {
			log.Printf("deadMemberMonitorTick failed with: %s", err)
		}
	}

	return nil
}

func deadMemberMonitorTick(ctx context.Context, node *flypg.Node, seenAt map[int]time.Time, deadMemberRemovalThreshold time.Duration) error {
	// TODO - We should connect using the flypgadmin user so we can  differentiate between
	// internal admin connection usage and the actual repmgr process.
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to open local connection: %s", err)
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
		sConn, err := node.RepMgr.NewRemoteConnection(ctx, standby.Hostname)
		if err != nil {
			// TODO - Verify the exception that's getting thrown.
			if time.Since(seenAt[standby.ID]) >= deadMemberRemovalThreshold {
				log.Printf("Removing dead member: %s\n", standby.Hostname)
				if err := node.RepMgr.UnregisterMember(ctx, standby); err != nil {
					log.Printf("failed to unregister member %s: %v", standby.Hostname, err)
					continue
				}
				delete(seenAt, standby.ID)
			}

			continue
		}
		defer sConn.Close(ctx)
		seenAt[standby.ID] = time.Now()
	}

	return nil
}
