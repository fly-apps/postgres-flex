package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func deadMemberMonitorTick(ctx context.Context, node *flypg.Node, seenAt map[int]time.Time, deadMemberRemovalThreshold time.Duration) error {
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

	return nil
}
