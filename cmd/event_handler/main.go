package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/api"
	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func main() {
	event := flag.String("event", "", "event type")
	nodeID := flag.Int("node-id", 0, "the node id")
	success := flag.String("success", "", "success (1) failure (0)")
	details := flag.String("details", "", "details")
	flag.Parse()

	succ := true
	if *success == "0" {
		succ = false
	}

	req := api.EventRequest{
		Name:    *event,
		NodeID:  *nodeID,
		Success: succ,
		Details: *details,
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Fatalln(err)
	}

	node, err := flypg.NewNode()
	if err != nil {
		log.Fatalln(err)
	}

	endpoint := fmt.Sprintf("http://[%s]:5500/commands/events/process", node.PrivateIP)
	resp, err := http.Post(endpoint, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		log.Fatalln(err)
	}

	if err := resp.Body.Close(); err != nil {
		log.Fatalln(err)
	}
}
