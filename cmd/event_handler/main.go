package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
)

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

	eventDetails := fmt.Sprintf("%s - Event: %s\n Node: %d\n Success: %s\n Details: %s\n", time.Now().String(), *event, *nodeID, *success, *details)

	// TODO - Use an actual logging framework instead of just writing strings to a file.
	logFile, err := os.OpenFile("/data/event.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("failed to open event log: %s", err)
	}
	defer logFile.Close()

	logFile.WriteString(eventDetails)

	switch *event {

	case "repmgrd_failover_promote", "standby_promote":
		// TODO - Need to figure out what to do when success == 0.

		retry := 0
		maxRetries := 5
		success := false

		for retry < maxRetries {
			if err := reconfigurePGBouncer(*nodeID); err != nil {
				errMsg := fmt.Sprintf("%s [%s] attempt: %d - failed to reconfigure pgbouncer: %s\n", *event, time.Now().String(), retry, err)
				logFile.WriteString(errMsg)

				retry++
				time.Sleep(1 * time.Second)
				continue
			}

			success = true
			break
		}

		if success {
			msg := fmt.Sprintf("%s [%s] Successfully reconfigured pgBouncer to %d\n", *event, time.Now().String(), *nodeID)
			logFile.WriteString(msg)
			os.Exit(0)
		} else {
			msg := fmt.Sprintf(" %s [%s] Failed ot reconfigured pgBouncer to %d\n", *event, time.Now().String(), *nodeID)
			logFile.WriteString(msg)
			os.Exit(1)
		}

	case "standby_follow":

		newMemberID, err := strconv.Atoi(*newPrimary)
		if err != nil {
			errMsg := fmt.Sprintf("failed to parse newMemberID %s: %s\n", *newPrimary, err)
			logFile.WriteString(errMsg)
			os.Exit(1)
		}

		retry := 0
		maxRetries := 5
		success := false

		for retry < maxRetries {
			if err := reconfigurePGBouncer(*&newMemberID); err != nil {
				errMsg := fmt.Sprintf("%s [%s] attempt: %d - failed to reconfigure pgbouncer: %s\n", *event, time.Now().String(), retry, err)
				logFile.WriteString(errMsg)

				retry++
				time.Sleep(1 * time.Second)
				continue
			}

			success = true
			break
		}

		if success {
			msg := fmt.Sprintf("%s [%s] Successfully reconfigured pgBouncer to %d\n", *event, time.Now().String(), newMemberID)
			logFile.WriteString(msg)
			os.Exit(0)
		} else {
			msg := fmt.Sprintf(" %s [%s] Failed ot reconfigured pgBouncer to %d\n", *event, time.Now().String(), newMemberID)
			logFile.WriteString(msg)
			os.Exit(1)
		}

	default:
		// noop
	}
}

func reconfigurePGBouncer(id int) error {
	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to reference node: %s", err)
	}

	conn, err := node.RepMgr.NewLocalConnection(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to establish connection with local pg: %s", err)
	}

	member, err := node.RepMgr.MemberByID(context.TODO(), conn, id)
	if err != nil {
		return err
	}

	if err := node.PGBouncer.ConfigurePrimary(context.TODO(), member.Hostname, true); err != nil {
		return fmt.Errorf("failed to reconfigure pgbouncer primary %s", err)
	}

	return nil
}
