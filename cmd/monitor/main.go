package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

var (
	deadMemberMonitorFrequency       = time.Hour * 1
	replicationStateMonitorFrequency = time.Hour * 1
	readonlyStateMonitorFrequency    = time.Minute * 1

	defaultDeadMemberRemovalThreshold   = time.Hour * 12
	defaultInactiveSlotRemovalThreshold = time.Hour * 12
)

func main() {
	ctx := context.Background()
	node, err := flypg.NewNode()
	if err != nil {
		fmt.Printf("failed to reference node: %s\n", err)
		os.Exit(1)
	}

	// Dead member monitor
	log.Println("Monitoring dead members")
	go monitorDeadMembers(ctx, node)

	// Readonly monitor
	log.Println("Monitoring readonly state")
	go monitorReadOnly(ctx, node)

	// Replication slot monitor
	log.Println("Monitoring replication slots")
	monitorReplicationSlots(ctx, node)
}
