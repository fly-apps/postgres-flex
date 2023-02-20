package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/jackc/pgx/v5"
)

const eventLogFile = "/data/event.log"

func main() {
	ctx := context.Background()

	if err := processEvent(ctx); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func processEvent(ctx context.Context) error {
	event := flag.String("event", "", "event type")
	nodeID := flag.Int("node-id", 0, "the node id")
	success := flag.String("success", "", "success (1) failure (0)")
	details := flag.String("details", "", "details")
	flag.Parse()

	logFile, err := os.OpenFile(eventLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to open event log: %s", err)
	}
	defer func() { _ = logFile.Close() }()

	log.SetOutput(logFile)
	log.Printf("event: %s, node: %d, success: %s, details: %s\n", *event, *nodeID, *success, *details)

	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	switch *event {
	case "child_node_disconnect", "child_node_reconnect", "child_node_new_connect":
		conn, err := node.RepMgr.NewLocalConnection(ctx)
		if err != nil {
			return fmt.Errorf("failed to open local connection: %s", err)
		}
		defer func() { _ = conn.Close(ctx) }()

		member, err := node.RepMgr.Member(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to resolve member: %s", err)
		}

		if member.Role != flypg.PrimaryRoleName {
			// We should never get here.
			log.Println("skipping since we are not the primary")
			return nil
		}

		if err := evaluateClusterState(ctx, conn, node); err != nil {
			return fmt.Errorf("failed to evaluate cluster state: %s", err)
		}
	}

	return logFile.Sync()
}

func evaluateClusterState(ctx context.Context, conn *pgx.Conn, node *flypg.Node) error {
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
		log.Println("Clearing zombie lock and re-enabling read/write")
		if err := flypg.RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock: %s", err)
		}

		log.Println("Broadcasting readonly state change")
		if err := flypg.BroadcastReadonlyChange(ctx, node, false); err != nil {
			log.Printf("failed to disable readonly: %s", err)
		}
	}

	return nil
}
