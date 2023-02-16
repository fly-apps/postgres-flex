package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

var (
	deadMemberMonitorFrequency       = time.Hour * 1
	replicationStateMonitorFrequency = time.Hour * 1
	clusterStateMonitorFrequency     = time.Minute * 15

	defaultDeadMemberRemovalThreshold   = time.Hour * 12
	defaultInactiveSlotRemovalThreshold = time.Hour * 12
)

func main() {
	ctx := context.Background()

	node, err := flypg.NewNode()
	if err != nil {
		panic(fmt.Sprintf("failed to reference node: %s\n", err))
	}

	// Dead member monitor
	log.Println("Monitoring dead members")
	go func() {
		if err := monitorDeadMembers(ctx, node); err != nil {
			panic(err)
		}
	}()

	// Readonly monitor
	log.Println("Monitoring cluster state")
	go monitorClusterState(ctx, node)

	// Replication slot monitor
	log.Println("Monitoring replication slots")
	monitorReplicationSlots(ctx, node)
}
