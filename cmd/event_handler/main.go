package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"

	"github.com/fly-apps/postgres-flex/pkg/flypg"

	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
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

		cs, err := state.NewClusterState()
		if err != nil {
			fmt.Printf("failed initialize cluster state store. %v", err)
		}

		member, err := cs.FindMember(int32(*nodeID))
		if err != nil {
			fmt.Printf("failed to find member %v: %s", *nodeID, err)
		}

		if err := cs.AssignPrimary(member.ID); err != nil {
			fmt.Printf("failed to register primary with consul: %s", err)
		}

		flypgNode, err := flypg.NewNode()
		if err != nil {
			fmt.Printf("failed to reference node: %s\n", err)
		}

		fmt.Println("Reconfiguring pgbouncer primary")
		if err := flypgNode.PGBouncer.ConfigurePrimary(context.TODO(), member.Hostname, true); err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	case "standby_follow":
		cs, err := state.NewClusterState()
		if err != nil {
			fmt.Printf("failed initialize cluster state store. %v", err)
		}

		newMemberID, err := strconv.Atoi(*newPrimary)
		if err != nil {
			fmt.Printf("failed to parse new member id: %s", err)
		}

		member, err := cs.FindMember(int32(newMemberID))
		if err != nil {
			fmt.Printf("failed to find member in consul: %s", err)
		}

		flypgNode, err := flypg.NewNode()
		if err != nil {
			fmt.Printf("failed to reference member: %s\n", err)
		}

		fmt.Println("Reconfiguring pgbouncer primary")
		if err := flypgNode.PGBouncer.ConfigurePrimary(context.TODO(), member.Hostname, true); err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	default:
		// noop
	}
}
