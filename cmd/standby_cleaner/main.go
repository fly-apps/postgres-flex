package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
)

var Minute int64 = 60

func main() {
	ctx := context.Background()
	flypgNode, err := flypg.NewNode()
	if err != nil {
		fmt.Printf("failed to reference node: %s\n", err)
		os.Exit(1)
	}

	conn, err := flypgNode.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		fmt.Printf("failed to open local connection: %s\n", err)
		os.Exit(1)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	seenAt := map[int]int64{}

	for _ = range ticker.C {
		role, err := flypgNode.RepMgr.CurrentRole(ctx, conn)
		if err != nil {
			fmt.Printf("Failed to check role: %s", err)
			continue
		}
		if role != "primary" {
			continue
		}
		standbys, err := flypgNode.RepMgr.Standbys(ctx, conn)
		if err != nil {
			fmt.Printf("Failed to get standbys: %s", err)
			continue
		}
		for _, standby := range standbys {
			newConn, err := flypgNode.RepMgr.NewRemoteConnection(ctx, standby.Ip)
			if err != nil {
				if time.Now().Unix()-seenAt[standby.Id] >= 10*Minute {
					cs, err := state.NewClusterState()
					if err != nil {
						fmt.Printf("failed initialize cluster state store. %v", err)
					}

					err = flypgNode.RepMgr.UnregisterStandby(standby.Id)
					if err != nil {
						fmt.Printf("Failed to unregister %d: %s", standby.Id, err)
						continue
					}
					delete(seenAt, standby.Id)

					// Remove from Consul
					if err = cs.UnregisterMember(int32(standby.Id)); err != nil {
						fmt.Printf("Failed to unregister %d from consul: %s", standby.Id, err)
					}
				}
			} else {
				seenAt[standby.Id] = time.Now().Unix()
				newConn.Close(ctx)
			}
		}
	}
}
