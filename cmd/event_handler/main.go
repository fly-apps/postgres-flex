package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"

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

	fmt.Printf("Event: %s\n Node: %d\n Success: %s\n Details: %s\n",
		*event, *nodeID, *success, *details)

	switch *event {
	case "repmgrd_failover_promote", "standby_promote":
		// TODO - Need to figure out what to do when success == 0.
		if err := reconfigurePGBouncer(*nodeID); err != nil {
			fmt.Println(err.Error())
			return
		}

	case "standby_follow":
		newMemberID, err := strconv.Atoi(*newPrimary)
		if err != nil {
			fmt.Printf("failed to parse new member id: %s", err)
		}

		if err := reconfigurePGBouncer(newMemberID); err != nil {
			fmt.Println(err.Error())
			return
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

	member, err := node.RepMgr.ResolveMemberByID(context.TODO(), conn, id)
	if err != nil {
		return err
	}

	fmt.Println("Reconfiguring pgbouncer primary")
	if err := node.PGBouncer.ConfigurePrimary(context.TODO(), member.Hostname, true); err != nil {
		return fmt.Errorf("failed to reconfigure pgbouncer primary %s", err)
	}

	return nil
}
