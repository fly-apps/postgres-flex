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

	// Wait for postgres to boot and become accessible.
	log.Println("Waiting for Postgres to be ready...")
	waitOnPostgres(ctx, node)
	log.Println("Postgres is ready to accept connections. Starting monitor...")

	// Dead member monitor
	go func() {
		if err := monitorDeadMembers(ctx, node); err != nil {
			panic(err)
		}
	}()

	// No need to monitor backups outside of the primary region.
	if os.Getenv("S3_ARCHIVE_CONFIG") != "" && node.PrimaryRegion == node.RepMgr.Region {
		store, err := state.NewStore()
		if err != nil {
			panic(fmt.Errorf("failed initialize cluster state store: %s", err))
		}

		barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
		if err != nil {
			panic(err)
		}

		if err := barman.LoadConfig(flypg.DefaultBarmanConfigDir); err != nil {
			panic(err)
		}

		// Backup scheduler
		go monitorBackupSchedule(ctx, node, barman)

		// Backup retention monitor
		go monitorBackupRetention(ctx, node, barman)
	}

	// Readonly monitor
	go monitorClusterState(ctx, node)

	// Replication slot monitor
	monitorReplicationSlots(ctx, node)
}

func waitOnPostgres(ctx context.Context, node *flypg.Node) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn, err := node.NewLocalConnection(ctx, "postgres", node.SUCredentials)
			if err != nil {
				log.Printf("failed to open local connection: %s", err)
				continue
			}
			defer func() { _ = conn.Close(ctx) }()

			if err := conn.Ping(ctx); err != nil {
				log.Printf("failed to ping local connection: %s", err)
				continue
			}

			return
		}
	}
}
