package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

type EventRequest struct {
	Name    string `json:"name"`
	NodeID  int    `json:"nodeID"`
	Success bool   `json:"success"`
	Details string `json:"details"`
}

const (
	childNodeDisconnect = "child_node_disconnect"
	childNodeReconnect  = "child_node_reconnect"
	childNodeNewConnect = "child_node_new_connect"
)

func handleEvent(w http.ResponseWriter, r *http.Request) {
	var event EventRequest
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		log.Printf("[ERROR] Failed to decode event request: %s\n", err)
		renderErr(w, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if !event.Success {
		errMsg := fmt.Sprintf("[ERROR] Event %q failed: %s", event.Name, event.Details)
		log.Println(errMsg)
		renderErr(w, errors.New(errMsg))
		return
	}

	if err := processEvent(r.Context(), event); err != nil {
		log.Printf("[ERROR] Failed to process event: %s\n", err)
		renderErr(w, err)
		return
	}
}

func processEvent(ctx context.Context, event EventRequest) error {
	log.Printf("Processing event: %q \n", event.Name)
	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	switch event.Name {
	case childNodeDisconnect, childNodeReconnect, childNodeNewConnect:
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
			return nil
		}

		if err := flypg.EvaluateClusterState(ctx, conn, node); err != nil {
			return fmt.Errorf("failed to evaluate cluster state: %s", err)
		}
	}

	return nil
}
