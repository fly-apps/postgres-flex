package main

import (
	"flag"
	"fmt"

	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
)

func main() {
	event := flag.String("event", "", "event type")
	nodeID := flag.Int("node-id", 0, "the node id")
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
	default:
		// noop
	}
}
