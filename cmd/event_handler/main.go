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
	event := flag.String("event", "", "event type")
	nodeID := flag.Int("node-id", 0, "the node id")
	success := flag.String("success", "", "success (1) failure (0)")
	details := flag.String("details", "", "details")
	flag.Parse()

	ctx := context.Background()

	logFile, err := os.OpenFile(eventLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to open event log: %s", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.Printf("event: %s, node: %d, success: %s, details: %s\n", *event, *nodeID, *success, *details)

	node, err := flypg.NewNode()
	if err != nil {
		log.Printf("failed to initialize node: %s", err)
		os.Exit(1)
	}

	switch *event {
	case "child_node_disconnect", "child_node_reconnect", "child_node_new_connect":
		conn, err := node.RepMgr.NewLocalConnection(ctx)
		if err != nil {
			log.Printf("failed to open local connection: %s", err)
			os.Exit(1)
		}
		defer conn.Close(ctx)

		member, err := node.RepMgr.Member(ctx, conn)
		if err != nil {
			log.Printf("failed to resolve member: %s", err)
			os.Exit(1)
		}

		if member.Role != flypg.PrimaryRoleName {
			// We should never get here.
			log.Println("skipping since we are not the primary")
			os.Exit(0)
		}

		if err := evaluateClusterState(ctx, conn, node); err != nil {
			log.Printf("failed to evaluate cluster state: %s", err)
			os.Exit(0)
		}

		os.Exit(0)
	default:
		// noop
	}
}

func evaluateClusterState(ctx context.Context, conn *pgx.Conn, node *flypg.Node) error {
	standbys, err := node.RepMgr.StandbyMembers(ctx, conn)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to query standbys")
		}
	}

	sample, err := flypg.TakeDNASample(ctx, node, standbys)
	if err != nil {
		return fmt.Errorf("failed to evaluate cluster data: %s", err)
	}

	log.Println(flypg.DNASampleString(sample))

	primary, err := flypg.ZombieDiagnosis(sample)
	if errors.Is(err, flypg.ErrZombieDiagnosisUndecided) || errors.Is(err, flypg.ErrZombieDiscovered) {
		// Quarantine primary
		if err := flypg.Quarantine(ctx, conn, node, primary); err != nil {
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
