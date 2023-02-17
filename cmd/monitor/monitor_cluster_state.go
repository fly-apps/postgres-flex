package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func monitorClusterState(ctx context.Context, node *flypg.Node) {
	ticker := time.NewTicker(clusterStateMonitorFrequency)
	defer ticker.Stop()
	for range ticker.C {
		if err := clusterStateMonitorTick(ctx, node); err != nil {
			log.Printf("clusterStateMonitorTick failed with: %s", err)
		}
	}
}

func clusterStateMonitorTick(ctx context.Context, node *flypg.Node) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to open local connection: %s", err)
	}
	defer conn.Close(ctx)

	member, err := node.RepMgr.Member(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query local member: %s", err)
	}

	// We only need to monitor the primary
	if member.Role != flypg.PrimaryRoleName {
		return nil
	}

	primary, err := flypg.PerformScreening(ctx, conn, node)
	if errors.Is(err, flypg.ErrZombieDiagnosisUndecided) || errors.Is(err, flypg.ErrZombieDiscovered) {
		if err := flypg.Quarantine(ctx, node, primary); err != nil {
			return fmt.Errorf("failed to quarantine failed primary: %s", err)
		}
		return fmt.Errorf("primary has been quarantined: %s", err)
	} else if err != nil {
		return fmt.Errorf("failed to run zombie diagnosis: %s", err)
	}

	// Clear zombie lock if it exists
	if flypg.ZombieLockExists() {
		log.Println("Clearing zombie lock and enabling read/write")
		if err := flypg.RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock: %s", err)
		}

		log.Println("Broadcasting readonly state change")
		if err := flypg.BroadcastReadonlyChange(ctx, node, false); err != nil {
			log.Printf("errors while disabling readonly: %s", err)
		}
	}

	return nil
}
