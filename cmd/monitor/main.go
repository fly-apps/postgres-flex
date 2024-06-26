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
	clusterStateMonitorFrequency     = time.Minute * 5

	defaultDeadMemberRemovalThreshold   = time.Hour * 12
	defaultInactiveSlotRemovalThreshold = time.Hour * 12

	defaultBackupRetentionEvaluationThreshold = time.Hour * 1
)

func main() {
	ctx := context.Background()

	log.SetFlags(0)

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

	if os.Getenv("CLOUD_ARCHIVING_ENABLED") == "true" {
		barman, err := flypg.NewBarman()
		if err != nil {
			panic(err)
		}

		log.Println("Monitoring backup retention")
		barman.PrintRetentionPolicy()

		go monitorBackupRetention(ctx, barman)
	}

	// Readonly monitor
	log.Println("Monitoring cluster state")
	go monitorClusterState(ctx, node)

	// Replication slot monitor
	log.Println("Monitoring replication slots")
	monitorReplicationSlots(ctx, node)
}
