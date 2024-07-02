package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

var (
	deadMemberMonitorFrequency       = time.Hour * 1
	replicationStateMonitorFrequency = time.Hour * 1
	clusterStateMonitorFrequency     = time.Minute * 5

	defaultDeadMemberRemovalThreshold   = time.Hour * 12
	defaultInactiveSlotRemovalThreshold = time.Hour * 12

	defaultBackupRetentionEvalFrequency = time.Hour * 12
	defaultFullBackupSchedule           = time.Hour * 24
)

// TODO - Harden this so one failure doesn't take down the whole monitor

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

	if os.Getenv("BARMAN_ENABLED") != "" {
		store, err := state.NewStore()
		if err != nil {
			panic(fmt.Errorf("failed initialize cluster state store: %s", err))
		}

		barman, err := flypg.NewBarman(store, os.Getenv("BARMAN_ENABLED"), flypg.DefaultAuthProfile)
		if err != nil {
			panic(err)
		}

		if err := barman.LoadConfig(flypg.DefaultBarmanConfigDir); err != nil {
			panic(err)
		}

		// Backup scheduler
		log.Println("Monitoring backup schedule")
		go monitorBackupSchedule(ctx, barman)

		// Backup retention monitor
		log.Println("Monitoring backup retention")
		go monitorBackupRetention(ctx, barman)
	}

	// Readonly monitor
	log.Println("Monitoring cluster state")
	go monitorClusterState(ctx, node)

	// Replication slot monitor
	log.Println("Monitoring replication slots")
	monitorReplicationSlots(ctx, node)
}
