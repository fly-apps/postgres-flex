package main

import (
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
		client, err := state.NewConsulClient()
		if err != nil {
			fmt.Printf("failed to initialize consul client: %s", err)
		}

		node, err := client.Node(int32(*nodeID))
		if err != nil {
			fmt.Printf("failed to find node: %s", err)
		}

		if err := client.RegisterPrimary(string(node.Value)); err != nil {
			fmt.Printf("failed to register primary: %s", err)
		}

		flypgNode, err := flypg.NewNode()
		if err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}

		fmt.Println("Reconfiguring pgbouncer primary")
		if err := flypgNode.PGBouncer.ConfigurePrimary(string(node.Value), true); err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	case "standby_follow":
		client, err := state.NewConsulClient()
		if err != nil {
			fmt.Printf("failed to initialize consul client: %s", err)
		}
		newNodeID, err := strconv.Atoi(*newPrimary)
		if err != nil {
			fmt.Printf("failed to parse new node id: %s", err)
		}
		node, err := client.Node(int32(newNodeID))
		if err != nil {
			fmt.Printf("failed to find node: %s", err)
		}
		flypgNode, err := flypg.NewNode()
		if err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
		fmt.Println("Reconfiguring pgbouncer primary")
		if err := flypgNode.PGBouncer.ConfigurePrimary(string(node.Value), true); err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	default:
		// noop
	}
}
