package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/jackc/pgx/v5"
)

const eventLogFile = "/data/event.log"

func main() {
	event := flag.String("event", "", "event type")
	nodeID := flag.Int("node-id", 0, "the node id")
	// This might not actually always be the new primary. %p from repmgr is variably the new or
	// old primary. In the events that we subscribe to it's always either empty or the new primary.
	// In the future if we subscribe to repmgrd_failover_promote, then we would have to change this
	// name.
	newPrimary := flag.String("new-node-id", "", "the new primary node id")
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

	switch *event {
	case "repmgrd_failover_promote", "standby_promote":
		// TODO - Need to figure out what to do when success == 0.
		retry := 0
		maxRetries := 5
		success := false

		for retry < maxRetries {
			if err := reconfigurePGBouncer(ctx, *nodeID); err != nil {
				log.Printf("%s - failed to reconfigure pgbouncer: %s. (attempt: %d)\n", *event, err, retry)
				retry++
				time.Sleep(1 * time.Second)
				continue
			}
			success = true
			break
		}

		if success {
			log.Printf("%s - successfully reconfigured pgbouncer to target: %d\n", *event, *nodeID)
			os.Exit(0)
		} else {
			log.Printf("%s - failed to reconfigured pgbouncer to target: %d\n", *event, *nodeID)
			os.Exit(1)
		}

	case "standby_follow":
		newMemberID, err := strconv.Atoi(*newPrimary)
		if err != nil {
			log.Printf("failed to parse newMemberID %s: %s\n", *newPrimary, err)
			os.Exit(1)
		}

		retry := 0
		maxRetries := 5
		success := false
		for retry < maxRetries {
			if err := reconfigurePGBouncer(ctx, newMemberID); err != nil {
				log.Printf("%s - failed to reconfigure pgbouncer: %s. (attempt: %d)\n", *event, err, retry)
				retry++
				time.Sleep(1 * time.Second)
				continue
			}
			success = true
			break
		}

		if success {
			log.Printf("%s - successfully reconfigured pgbouncer to target: %d\n", *event, newMemberID)
			os.Exit(0)
		} else {
			log.Printf("%s - failed to reconfigured pgbouncer to target: %d\n", *event, newMemberID)
			os.Exit(1)
		}

	case "child_node_disconnect", "child_node_reconnect", "child_node_new_connect":
		node, err := flypg.NewNode()
		if err != nil {
			log.Printf("failed to initialize node: %s", err)
			os.Exit(1)
		}

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

func reconfigurePGBouncer(ctx context.Context, id int) error {
	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to reference node: %s", err)
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection with local pg: %s", err)
	}

	member, err := node.RepMgr.MemberByID(ctx, conn, id)
	if err != nil {
		return err
	}

	if err := node.PGBouncer.ConfigurePrimary(ctx, member.Hostname, true); err != nil {
		return fmt.Errorf("failed to reconfigure pgbouncer primary %s", err)
	}

	return nil
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
		if err := flypg.Quarantine(ctx, conn, node, primary); err != nil {
			return fmt.Errorf("failed to quarantine failed primary: %s", err)
		}

		return fmt.Errorf("primary has been quarantined: %s", err)
	} else if err != nil {
		return fmt.Errorf("failed to run zombie diagnosis: %s", err)
	}

	// If the zombie lock exists clear it
	if flypg.ZombieLockExists() {
		log.Println("Clearing zombie lock and turning read/write")
		if err := flypg.RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock: %s", err)
		}

		if err := flypg.UnsetReadOnly(ctx, node, conn); err != nil {
			return fmt.Errorf("failed to unset readonly: %s", err)
		}
	}

	return nil
}
